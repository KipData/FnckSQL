use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::planner::operator::truncate::TruncateOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use sqlparser::ast::ObjectName;
use std::sync::Arc;

impl<T: Transaction> Binder<'_, '_, T> {
    pub(crate) fn bind_truncate(
        &mut self,
        name: &ObjectName,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name = Arc::new(lower_case_name(name)?);

        Ok(LogicalPlan::new(
            Operator::Truncate(TruncateOperator { table_name }),
            Childrens::None,
        ))
    }
}
