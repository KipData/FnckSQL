use crate::optimizer::core::pattern::PatternMatcher;
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::heuristic::batch::HepMatchOrder;
use crate::optimizer::heuristic::graph::HepGraph;
use crate::optimizer::heuristic::matcher::HepMatcher;
use crate::optimizer::OptimizerError;
use crate::optimizer::rule::implementation::ImplementationRuleImpl;
use crate::planner::operator::PhysicalOption;

#[derive(Debug, Clone)]
pub struct Expression {
    pub(crate) ops: Vec<PhysicalOption>,
}

#[derive(Debug, Clone)]
pub struct GroupExpression {
    exprs: Vec<Expression>
}

impl GroupExpression {
    pub(crate) fn append_expr(&mut self, expr: Expression) {
        self.exprs.push(expr);
    }
}

#[derive(Debug)]
pub struct Memo {
    groups: Vec<GroupExpression>
}

impl Memo {
    pub(crate) fn new(graph: &HepGraph, implementations: &[ImplementationRuleImpl]) -> Result<Self, OptimizerError> {
        let node_count = graph.node_count();
        let mut groups = vec![GroupExpression { exprs: Vec::new() }; node_count];

        if node_count == 0 {
            return Err(OptimizerError::EmptyPlan);
        }

        for node_id in graph.nodes_iter(HepMatchOrder::TopDown, None) {
            for rule in implementations {
                if HepMatcher::new(rule.pattern(), node_id, graph).match_opt_expr() {
                    let op = graph.operator(node_id);

                    rule.to_expression(op, &mut groups[node_id.index()])?;
                }
            }
        }

        Ok(Memo { groups })
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::test::select_sql_run;
    use crate::db::DatabaseError;
    use crate::optimizer::core::memo::Memo;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::graph::HepGraph;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::implementation::ImplementationRuleImpl;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;

    #[tokio::test]
    async fn test_build_memo() -> Result<(), DatabaseError> {
        let plan = select_sql_run("select c1, c3 from t1 inner join t2 on c1 = c3 where c1 > 10 and c3 > 22").await?;
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

        let memo = Memo::new(&graph, &rules)?;

        println!("{:#?}", memo);

        Ok(())
    }
}
