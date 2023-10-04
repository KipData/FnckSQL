use crate::binder::{lower_case_name, split_name, BindError, Binder};
use crate::planner::operator::truncate::TruncateOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Storage;
use sqlparser::ast::ObjectName;
use std::sync::Arc;

impl<S: Storage> Binder<S> {
    pub(crate) async fn bind_truncate(
        &mut self,
        name: &ObjectName,
    ) -> Result<LogicalPlan, BindError> {
        let name = lower_case_name(&name);
        let (_, name) = split_name(&name)?;
        let table_name = Arc::new(name.to_string());

        let plan = LogicalPlan {
            operator: Operator::Truncate(TruncateOperator { table_name }),
            childrens: vec![],
        };
        Ok(plan)
    }
}
