use crate::binder::{lower_case_name, split_name, BindError, Binder};
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::operator::scan::ScanOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use sqlparser::ast::{Expr, TableFactor, TableWithJoins};
use std::sync::Arc;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_delete(
        &mut self,
        from: &TableWithJoins,
        selection: &Option<Expr>,
    ) -> Result<LogicalPlan, BindError> {
        if let TableFactor::Table { name, alias, .. } = &from.relation {
            let name = lower_case_name(name);
            let name = split_name(&name)?;
            let table_name = Arc::new(name.to_string());

            let table_catalog = self
                .context
                .table(table_name.clone())
                .cloned()
                .ok_or_else(|| BindError::InvalidTable(format!("bind table {}", name)))?;
            let primary_key_column = table_catalog
                .all_columns_with_id()
                .iter()
                .find(|(_, column)| column.desc.is_primary)
                .map(|(_, column)| Arc::clone(column))
                .unwrap();
            let mut plan = ScanOperator::build(table_name.clone(), &table_catalog);

            self.context
                .add_bind_table(table_name.clone(), table_catalog, None)?;

            if let Some(alias) = alias {
                self.context
                    .add_table_alias(alias.to_string(), table_name.clone())?;
            }

            if let Some(predicate) = selection {
                plan = self.bind_where(plan, predicate)?;
            }

            Ok(LogicalPlan {
                operator: Operator::Delete(DeleteOperator {
                    table_name,
                    primary_key_column,
                }),
                childrens: vec![plan],
                physical_option: None,
            })
        } else {
            unreachable!("only table")
        }
    }
}
