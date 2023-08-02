use std::sync::Arc;

use crate::{
    expression::ScalarExpression,
    planner::operator::Operator,
};
use crate::planner::LogicalPlan;

#[derive(Debug, PartialEq, Clone)]
pub struct AggregateOperator {
    pub groupby_exprs: Vec<ScalarExpression>,

    pub agg_calls: Vec<ScalarExpression>,
}

impl AggregateOperator {
    pub fn new(
        children: LogicalPlan,
        agg_calls: Vec<ScalarExpression>,
        groupby_exprs: Vec<ScalarExpression>,
    ) -> LogicalPlan {
        LogicalPlan {
            operator: Arc::new(Operator::Aggregate(Self {
                groupby_exprs,
                agg_calls,
            })),
            children: vec![Arc::new(children)],
        }
    }
}
