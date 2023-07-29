use std::sync::Arc;
use anyhow::Result;
use crate::planner::operator::Operator;

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalInsertPlan {
    pub operator: Arc<Operator>,
    pub children: Vec<Arc<LogicalInsertPlan>>,
}

impl LogicalInsertPlan {
    pub fn child(&self, index: usize) -> Result<&LogicalInsertPlan> {
        self.children
            .get(index)
            .map(|v| v.as_ref())
            .ok_or_else(|| anyhow::Error::msg(format!("Invalid children index {}", index)))
    }
}