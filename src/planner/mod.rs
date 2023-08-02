pub mod display;
pub mod logical_plan_builder;
pub mod operator;

use std::sync::Arc;
use anyhow::Result;
use crate::planner::operator::OperatorRef;

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalPlan {
    pub operator: OperatorRef,
    pub children: Vec<Arc<LogicalPlan>>,
}

impl LogicalPlan {
    pub fn child(&self, index: usize) -> Result<&LogicalPlan> {
        self.children
            .get(index)
            .map(|v| v.as_ref())
            .ok_or_else(|| anyhow::Error::msg(format!("Invalid children index {}", index)))
    }
}
