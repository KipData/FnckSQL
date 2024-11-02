use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::planner::operator::describe::DescribeOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use sqlparser::ast::ObjectName;
use std::sync::Arc;

impl<T: Transaction> Binder<'_, '_, T> {
    pub(crate) fn bind_describe(
        &mut self,
        name: &ObjectName,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name = Arc::new(lower_case_name(name)?);

        Ok(LogicalPlan::new(
            Operator::Describe(DescribeOperator { table_name }),
            vec![],
        ))
    }
}
