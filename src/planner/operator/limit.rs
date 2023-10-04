use crate::planner::LogicalPlan;

use super::Operator;

#[derive(Debug, PartialEq, Clone)]
pub struct LimitOperator {
    pub offset: usize,
    pub limit: usize,
}

impl LimitOperator {
    pub fn new(offset: usize, limit: usize, children: LogicalPlan) -> LogicalPlan {
        LogicalPlan {
            operator: Operator::Limit(LimitOperator { offset, limit }),
            childrens: vec![children],
        }
    }
}
