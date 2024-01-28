use crate::optimizer::core::column_meta::ColumnMetaLoader;
use crate::optimizer::core::pattern::PatternMatcher;
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::heuristic::batch::HepMatchOrder;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::heuristic::matcher::HepMatcher;
use crate::optimizer::rule::implementation::ImplementationRuleImpl;
use crate::optimizer::OptimizerError;
use crate::planner::operator::PhysicalOption;
use crate::storage::Transaction;
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Expression {
    pub(crate) op: PhysicalOption,
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
    groups: HashMap<HepNodeId, GroupExpression>,
}

impl Memo {
    pub(crate) fn new<T: Transaction>(
        graph: &HepGraph,
        loader: &ColumnMetaLoader<'_, T>,
        implementations: &[ImplementationRuleImpl],
    ) -> Result<Self, OptimizerError> {
        let node_count = graph.node_count();
        let mut groups = HashMap::new();

        if node_count == 0 {
            return Err(OptimizerError::EmptyPlan);
        }

        for node_id in graph.nodes_iter(HepMatchOrder::TopDown, None) {
            for rule in implementations {
                if HepMatcher::new(rule.pattern(), node_id, graph).match_opt_expr() {
                    let op = graph.operator(node_id);
                    let group_expr = groups
                        .entry(node_id)
                        .or_insert_with(|| GroupExpression { exprs: vec![] });

                    rule.to_expression(op, loader, group_expr)?;
                }
            }
        }

        Ok(Memo { groups })
    }

    pub(crate) fn cheapest_physical_option(&self, node_id: &HepNodeId) -> Option<PhysicalOption> {
        self.groups.get(node_id).and_then(|exprs| {
            exprs
                .exprs
                .iter()
                .min_by(|expr_1, expr_2| match (expr_1.cost, expr_2.cost) {
                    (Some(cost_1), Some(cost_2)) => cost_1.cmp(&cost_2),
                    (None, Some(_)) => Ordering::Greater,
                    (Some(_), None) => Ordering::Less,
                    (None, None) => Ordering::Equal,
                })
                .map(|expr| expr.op.clone())
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::{Binder, BinderContext};
    use crate::db::{Database, DatabaseError};
    use crate::optimizer::core::memo::Memo;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::graph::HepGraph;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::implementation::ImplementationRuleImpl;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::PhysicalOption;
    use crate::storage::kip::KipTransaction;
    use crate::storage::{Storage, Transaction};
    use petgraph::stable_graph::NodeIndex;
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
            // FIXME: Only by bracketing (c1 > 40 or c1 = 2) can the filter be pushed down below the join
            "select c1, c3 from t1 inner join t2 on c1 = c3 where (c1 > 40 or c1 = 2) and c3 > 22",
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
            .find_best::<KipTransaction>(None)?;
        let graph = HepGraph::new(best_plan);
        let rules = vec![
            ImplementationRuleImpl::Projection,
            ImplementationRuleImpl::Filter,
            ImplementationRuleImpl::HashJoin,
            ImplementationRuleImpl::SeqScan,
            ImplementationRuleImpl::IndexScan,
        ];

        let memo = Memo::new(&graph, &transaction.meta_loader(), &rules)?;
        let best_plan = graph.to_plan(Some(&memo));
        let exprs = &memo.groups.get(&NodeIndex::new(3)).unwrap();

        assert_eq!(exprs.exprs.len(), 2);
        assert_eq!(exprs.exprs[0].cost, Some(1000));
        assert_eq!(exprs.exprs[0].op, PhysicalOption::SeqScan);
        assert!(exprs.exprs[1].cost.unwrap() >= 1920);
        assert!(matches!(exprs.exprs[1].op, PhysicalOption::IndexScan(_)));
        assert_eq!(
            best_plan.as_ref().unwrap().childrens[0].childrens[0].childrens[0].physical_option,
            Some(PhysicalOption::SeqScan)
        );

        Ok(())
    }
}
