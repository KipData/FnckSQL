use std::sync::Arc;

use crate::{
    expression::ScalarExpression,
    planner::{logical_select_plan::LogicalSelectPlan, operator::Operator},
};

#[derive(Debug)]
pub struct AggregateOperator {
    pub groupby_exprs: Vec<ScalarExpression>,

    pub agg_calls: Vec<ScalarExpression>,
}

impl AggregateOperator {
    pub fn new(
        children: LogicalSelectPlan,
        agg_calls: Vec<ScalarExpression>,
        groupby_exprs: Vec<ScalarExpression>,
    ) -> LogicalSelectPlan {
        LogicalSelectPlan {
            operator: Arc::new(Operator::Aggregate(Self {
                groupby_exprs,
                agg_calls,
            })),
            children: vec![Arc::new(children)],
        }
    }
}
