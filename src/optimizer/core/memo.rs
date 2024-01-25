use crate::optimizer::core::histogram::HistogramLoader;
use crate::optimizer::core::pattern::PatternMatcher;
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::heuristic::batch::HepMatchOrder;
use crate::optimizer::heuristic::graph::HepGraph;
use crate::optimizer::heuristic::matcher::HepMatcher;
use crate::optimizer::rule::implementation::ImplementationRuleImpl;
use crate::optimizer::OptimizerError;
use crate::planner::operator::PhysicalOption;
use crate::storage::Transaction;

#[derive(Debug, Clone)]
pub struct Expression {
    pub(crate) ops: Vec<PhysicalOption>,
    pub(crate) cost: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct GroupExpression {
    exprs: Vec<Expression>,
}

impl GroupExpression {
    pub(crate) fn append_expr(&mut self, expr: Expression) {
        self.exprs.push(expr);
    }
}

#[derive(Debug)]
pub struct Memo {
    groups: Vec<GroupExpression>,
}

impl Memo {
    pub(crate) fn new<T: Transaction>(
        graph: &HepGraph,
        loader: &HistogramLoader<'_, T>,
        implementations: &[ImplementationRuleImpl],
    ) -> Result<Self, OptimizerError> {
        let node_count = graph.node_count();
        let mut groups = vec![GroupExpression { exprs: Vec::new() }; node_count];

        if node_count == 0 {
            return Err(OptimizerError::EmptyPlan);
        }

        for node_id in graph.nodes_iter(HepMatchOrder::TopDown, None) {
            for rule in implementations {
                if HepMatcher::new(rule.pattern(), node_id, graph).match_opt_expr() {
                    let op = graph.operator(node_id);

                    rule.to_expression(op, loader, &mut groups[node_id.index()])?;
                }
            }
        }

        Ok(Memo { groups })
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::{Binder, BinderContext};
    use crate::db::{Database, DatabaseError};
    use crate::optimizer::core::histogram::HistogramLoader;
    use crate::optimizer::core::memo::Memo;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::graph::HepGraph;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::implementation::ImplementationRuleImpl;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::storage::Storage;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_build_memo() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let database = Database::with_kipdb(temp_dir.path()).await?;
        database
            .run("create table t1 (c1 int primary key, c2 int)")
            .await?;
        database
            .run("create table t2 (c3 int primary key, c4 int)")
            .await?;

        for i in 0..1000 {
            let _ = database
                .run(format!("insert into t1 values({}, {})", i, i + 1).as_str())
                .await?;
        }
        database.run("analyze table t1").await?;

        let transaction = database.storage.transaction().await?;
        let binder = Binder::new(BinderContext::new(&transaction));
        let stmt = crate::parser::parse_sql(
            "select c1, c3 from t1 inner join t2 on c1 = c3 where c1 > 40 and c3 > 22",
        )?;
        let plan = binder.bind(&stmt[0])?;
        let best_plan = HepOptimizer::new(plan)
            .batch(
                "Simplify Filter".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::SimplifyFilter],
            )
            .batch(
                "Predicate Pushdown".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    NormalizationRuleImpl::PushPredicateThroughJoin,
                    NormalizationRuleImpl::PushPredicateIntoScan,
                ],
            )
            .find_best()?;
        let graph = HepGraph::new(best_plan);
        let rules = vec![
            ImplementationRuleImpl::Projection,
            ImplementationRuleImpl::Filter,
            ImplementationRuleImpl::HashJoin,
            ImplementationRuleImpl::SeqScan,
            ImplementationRuleImpl::IndexScan,
        ];

        let memo = Memo::new(&graph, &HistogramLoader::new(&transaction)?, &rules)?;

        println!("{:#?}", memo);

        Ok(())
    }
}
