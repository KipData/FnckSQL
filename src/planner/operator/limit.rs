use crate::planner::LogicalPlan;

use super::Operator;

#[derive(Debug, PartialEq, Clone)]
pub struct LimitOperator {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}

impl LimitOperator {
    pub fn new(offset: Option<usize>, limit: Option<usize>, children: LogicalPlan) -> LogicalPlan {
        LogicalPlan {
            operator: Operator::Limit(LimitOperator { offset, limit }),
            childrens: vec![children],
        }
    }
}
