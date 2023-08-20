pub mod display;
pub mod operator;

use crate::planner::operator::Operator;

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalPlan {
    pub operator: Operator,
    pub childrens: Vec<LogicalPlan>,
}

impl LogicalPlan {
    pub fn child(&self, index: usize) -> Option<&LogicalPlan> {
        self.childrens
            .get(index)
    }
}
