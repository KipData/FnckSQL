pub mod display;
pub mod operator;

use anyhow::Result;
use crate::planner::operator::Operator;

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalPlan {
    pub operator: Operator,
    pub childrens: Vec<LogicalPlan>,
}

impl LogicalPlan {
    pub fn child(&self, index: usize) -> Result<&LogicalPlan> {
        self.childrens
            .get(index)
            .ok_or_else(|| anyhow::Error::msg(format!("Invalid children index {}", index)))
    }
}
