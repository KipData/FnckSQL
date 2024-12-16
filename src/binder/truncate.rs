use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::planner::operator::truncate::TruncateOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use sqlparser::ast::ObjectName;
use std::sync::Arc;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
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
