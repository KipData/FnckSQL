use lazy_static::lazy_static;
use crate::optimizer::core::opt_expr::OptExprNode;
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::OptimizerError;
use crate::planner::operator::Operator;
lazy_static! {
    static ref SIMPLIFY_FILTER_RULE: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Filter(_)),
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Copy, Clone)]
pub struct SimplifyFilter;

impl Rule for SimplifyFilter {
    fn pattern(&self) -> &Pattern {
        &SIMPLIFY_FILTER_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        if let Operator::Filter(mut filter_op) = graph.operator(node_id).clone() {
            filter_op.predicate.simplify()?;

            graph.replace_node(
                node_id,
                OptExprNode::OperatorRef(Operator::Filter(filter_op))
            )
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::binder::test::select_sql_run;
    use crate::db::DatabaseError;
    use crate::expression::simplify::ConstantBinary;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::RuleImpl;
    use crate::planner::LogicalPlan;
    use crate::planner::operator::Operator;

    #[tokio::test]
    async fn test_simplify_filter() -> Result<(), DatabaseError> {
        // c1 + 1 < -1 -> c1 < -2
        let plan_1 = select_sql_run("select * from t1 where -(c1 + 1) > 1").await?;
        // 1 - c1 < -1 -> c1 > 2
        let plan_2 = select_sql_run("select * from t1 where -(1 - c1) > 1").await?;
        // c1 < -1
        let plan_3 = select_sql_run("select * from t1 where -c1 > 1").await?;
        // c1 > 0
        let plan_4 = select_sql_run("select * from t1 where c1 + 1 > 1").await?;

        // c1 + 1 < -1 -> c1 < -2
        let plan_5 = select_sql_run("select * from t1 where 1 < -(c1 + 1)").await?;
        // 1 - c1 < -1 -> c1 > 2
        let plan_6 = select_sql_run("select * from t1 where 1 < -(1 - c1)").await?;
        // c1 < -1
        let plan_7 = select_sql_run("select * from t1 where 1 < -c1").await?;
        // c1 > 0
        let plan_8 = select_sql_run("select * from t1 where 1 < c1 + 1").await?;

        let op = |plan: LogicalPlan, expr: &str| -> Result<Option<ConstantBinary>, DatabaseError> {
            let best_plan = HepOptimizer::new(plan.clone())
                .batch(
                    "test_simplify_filter".to_string(),
                    HepBatchStrategy::once_topdown(),
                    vec![RuleImpl::SimplifyFilter]
                )
                .find_best()?;
            if let Operator::Filter(filter_op) = best_plan.childrens[0].clone().operator {
                println!("{expr}: {:#?}", filter_op.predicate.convert_binary(&0).unwrap());

                Ok(filter_op.predicate.convert_binary(&0).unwrap())
            } else {
                Ok(None)
            }
        };

        assert_eq!(op(plan_1, "-(c1 + 1) > 1")?, op(plan_5, "1 < -(c1 + 1)")?);
        assert_eq!(op(plan_2, "-(1 - c1) > 1")?, op(plan_6, "1 < -(1 - c1)")?);
        assert_eq!(op(plan_3, "-c1 > 1")?, op(plan_7, "1 < -c1")?);
        assert_eq!(op(plan_4, "c1 + 1 > 1")?, op(plan_8, "1 < c1 + 1")?);

        Ok(())
    }
}