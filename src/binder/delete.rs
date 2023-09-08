use sqlparser::ast::{Expr, TableFactor, TableWithJoins};
use crate::binder::{Binder, BindError, lower_case_name, split_name};
use crate::planner::LogicalPlan;
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::operator::Operator;
use crate::storage::Storage;

impl<S: Storage> Binder<S> {
    pub(crate) async fn bind_delete(
        &mut self,
        from: &TableWithJoins,
        selection: &Option<Expr>,
    ) -> Result<LogicalPlan, BindError> {
        if let TableFactor::Table { name, .. } = &from.relation {
            let name = lower_case_name(name);
            let (_, name) = split_name(&name)?;
            let (table_name, mut plan) = self._bind_single_table_ref(None, name).await?;

            if let Some(predicate) = selection {
                plan = self.bind_where(plan, predicate).await?;
            }

            Ok(LogicalPlan {
                operator: Operator::Delete(
                    DeleteOperator {
                        table_name
                    }
                ),
                childrens: vec![plan],
            })
        } else {
            unreachable!("only table")
        }
    }
}