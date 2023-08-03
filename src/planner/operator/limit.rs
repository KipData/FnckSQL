use std::sync::Arc;

use crate::planner::logical_select_plan::LogicalSelectPlan;

use super::Operator;

#[derive(Debug, PartialEq, Clone)]
pub struct LimitOperator {
    pub offset: usize,
    pub limit: usize,
}

impl LimitOperator {
    pub fn new(offset: usize, limit: usize, children: LogicalSelectPlan) -> LogicalSelectPlan {
        LogicalSelectPlan {
            operator: Arc::new(Operator::Limit(LimitOperator { offset, limit })),
            children: vec![Arc::new(children)],
        }
    }
}
