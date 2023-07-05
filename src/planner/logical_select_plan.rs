use std::sync::Arc;

use super::operator::OperatorRef;
use anyhow::Result;

/// LogicalSelectPlan is a tree of operators that represent a logical query plan.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalSelectPlan {
    pub operator: OperatorRef,
    pub children: Vec<Arc<LogicalSelectPlan>>,
}

impl LogicalSelectPlan {
    pub fn child(&self, index: usize) -> Result<&LogicalSelectPlan> {
        self.children
            .get(index)
            .map(|v| v.as_ref())
            .ok_or_else(|| anyhow::Error::msg(format!("Invalid children index {}", index)))
    }
}
