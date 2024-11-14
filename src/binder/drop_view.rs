use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::planner::operator::drop_view::DropViewOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use sqlparser::ast::ObjectName;
use std::sync::Arc;

impl<T: Transaction> Binder<'_, '_, T> {
    pub(crate) fn bind_drop_view(
        &mut self,
        name: &ObjectName,
        if_exists: &bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        let view_name = Arc::new(lower_case_name(name)?);

        Ok(LogicalPlan::new(
            Operator::DropView(DropViewOperator {
                view_name,
                if_exists: *if_exists,
            }),
            vec![],
        ))
    }
}
