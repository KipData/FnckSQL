use crate::binder::Binder;
use crate::errors::DatabaseError;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;

impl<'a, 'b, T: Transaction> Binder<'a, 'b, T> {
    pub(crate) fn bind_explain(&mut self, plan: LogicalPlan) -> Result<LogicalPlan, DatabaseError> {
        Ok(LogicalPlan::new(Operator::Explain, vec![plan]))
    }
}
