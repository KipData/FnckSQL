use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::operator::scan::ScanOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use sqlparser::ast::{Expr, TableAlias, TableFactor, TableWithJoins};
use std::sync::Arc;

impl<'a, 'b, T: Transaction> Binder<'a, 'b, T> {
    pub(crate) fn bind_delete(
        &mut self,
        from: &TableWithJoins,
        selection: &Option<Expr>,
    ) -> Result<LogicalPlan, DatabaseError> {
        if let TableFactor::Table { name, alias, .. } = &from.relation {
            let table_name = Arc::new(lower_case_name(name)?);
            let mut table_alias = None;
            let mut alias_idents = None;

            if let Some(TableAlias { name, columns }) = alias {
                table_alias = Some(Arc::new(name.value.to_lowercase()));
                alias_idents = Some(columns);
            }
            let table_catalog =
                self.context
                    .table_and_bind(table_name.clone(), table_alias.clone(), None)?;
            let primary_key_column = table_catalog
                .columns()
                .find(|column| column.desc.is_primary)
                .cloned()
                .unwrap();
            let mut plan = ScanOperator::build(table_name.clone(), table_catalog);

            if let Some(alias_idents) = alias_idents {
                plan =
                    self.bind_alias(plan, alias_idents, table_alias.unwrap(), table_name.clone())?;
            }

            if let Some(predicate) = selection {
                plan = self.bind_where(plan, predicate)?;
            }

            Ok(LogicalPlan::new(
                Operator::Delete(DeleteOperator {
                    table_name,
                    primary_key_column,
                }),
                vec![plan],
            ))
        } else {
            unreachable!("only table")
        }
    }
}
