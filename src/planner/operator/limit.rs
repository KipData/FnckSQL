use std::sync::Arc;

use crate::planner::logical_select_plan::LogicalSelectPlan;

use super::Operator;

#[derive(Debug)]
pub struct LimitOperator {
    pub offset: usize,
    pub count: usize,
}

impl LimitOperator {
    pub fn new(offset: usize, count: usize, children: LogicalSelectPlan) -> LogicalSelectPlan {
        LogicalSelectPlan {
            operator: Arc::new(Operator::Limit(LimitOperator { offset, count })),
            children: vec![Arc::new(children)],
        }
    }
}
