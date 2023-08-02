use std::sync::Arc;

use crate::planner::LogicalPlan;

use super::Operator;

#[derive(Debug, PartialEq, Clone)]
pub struct LimitOperator {
    pub offset: usize,
    pub limit: usize,
}

impl LimitOperator {
pub fn new(offset: usize, count: usize, children: LogicalPlan) -> LogicalPlan {
        LogicalPlan {
            operator: Arc::new(Operator::Limit(LimitOperator { offset, count })),
            children: vec![Arc::new(children)],
        }
    }
}
