use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::planner::operator::drop_table::DropTableOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use sqlparser::ast::ObjectName;
use std::sync::Arc;

impl<'a, 'b, T: Transaction> Binder<'a, 'b, T> {
    pub(crate) fn bind_drop_table(
        &mut self,
        name: &ObjectName,
        if_exists: &bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name = Arc::new(lower_case_name(name)?);

        let plan = LogicalPlan::new(
            Operator::DropTable(DropTableOperator {
                table_name,
                if_exists: *if_exists,
            }),
            vec![],
        );
        Ok(plan)
    }
}
