use crate::binder::{BindError, Binder};
use crate::planner::operator::show::ShowTablesOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_show_tables(&mut self) -> Result<LogicalPlan, BindError> {
        let plan = LogicalPlan {
            operator: Operator::Show(ShowTablesOperator {}),
            childrens: vec![],
            physical_option: None,
        };
        Ok(plan)
    }
}
