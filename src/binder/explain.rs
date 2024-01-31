use crate::binder::{BindError, Binder};
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_explain(&mut self, plan: LogicalPlan) -> Result<LogicalPlan, BindError> {
        Ok(LogicalPlan {
            operator: Operator::Explain,
            childrens: vec![plan],
            physical_option: None,
        })
    }
}
