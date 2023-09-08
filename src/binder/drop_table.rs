use std::sync::Arc;
use sqlparser::ast::ObjectName;
use crate::binder::{Binder, BindError, lower_case_name, split_name};
use crate::planner::LogicalPlan;
use crate::planner::operator::drop_table::DropTableOperator;
use crate::planner::operator::Operator;
use crate::storage::Storage;

impl<S: Storage> Binder<S> {
    pub(crate) fn bind_drop_table(
        &mut self,
        name: &ObjectName
    ) -> Result<LogicalPlan, BindError> {
        let name = lower_case_name(&name);
        let (_, name) = split_name(&name)?;
        let table_name = Arc::new(name.to_string());

        let plan = LogicalPlan {
            operator: Operator::DropTable(
                DropTableOperator {
                    table_name
                }
            ),
            childrens: vec![],
        };
        Ok(plan)
    }
}