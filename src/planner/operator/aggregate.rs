use crate::planner::LogicalPlan;
use crate::{expression::ScalarExpression, planner::operator::Operator};

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct AggregateOperator {
    pub groupby_exprs: Vec<ScalarExpression>,
    pub agg_calls: Vec<ScalarExpression>,
}

impl AggregateOperator {
    pub fn build(
        children: LogicalPlan,
        agg_calls: Vec<ScalarExpression>,
        groupby_exprs: Vec<ScalarExpression>,
    ) -> LogicalPlan {
        LogicalPlan {
            operator: Operator::Aggregate(Self {
                groupby_exprs,
                agg_calls,
            }),
            childrens: vec![children],
            physical_option: None,
        }
    }
}
