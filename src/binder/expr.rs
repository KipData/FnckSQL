use crate::binder::BindError;
use anyhow::Result;
use itertools::Itertools;
use sqlparser::ast::{Expr, Ident};
use std::slice;

use super::Binder;
use crate::expression::ScalarExpression;

impl Binder {
    pub(crate) fn bind_expr(&mut self, expr: &Expr) -> Result<ScalarExpression> {
        match expr {
            Expr::Identifier(ident) => {
                self.bind_column_ref_from_identifiers(slice::from_ref(ident))
            }
            _ => {
                todo!()
            }
        }
    }

    pub fn bind_column_ref_from_identifiers(
        &mut self,
        idents: &[Ident],
    ) -> Result<ScalarExpression> {
        let idents = idents
            .iter()
            .map(|ident| Ident::new(ident.value.to_lowercase()))
            .collect_vec();
        let (_schema_name, table_name, column_name) = match idents.as_slice() {
            [column] => (None, None, &column.value),
            [table, column] => (None, Some(&table.value), &column.value),
            [schema, table, column] => (Some(&schema.value), Some(&table.value), &column.value),
            _ => {
                return Err(BindError::InvalidColumn(
                    idents
                        .iter()
                        .map(|ident| ident.value.clone())
                        .join(".")
                        .to_string(),
                )
                .into())
            }
        };

        if let Some(table) = table_name {
            let table_catalog = self
                .context
                .catalog
                .get_table_by_name(table)
                .ok_or_else(|| BindError::InvalidTable(table.to_string()))?;

            let column_catalog = table_catalog
                .get_column_by_name(column_name)
                .ok_or_else(|| BindError::InvalidColumn(column_name.to_string()))?;
            Ok(ScalarExpression::ColumnRef(column_catalog.clone()))
        } else {
            // handle col syntax
            let mut got_column = None;
            for table_catalog in &self.context.catalog.tables {
                if let Some(column_catalog) = table_catalog.get_column_by_name(column_name) {
                    if got_column.is_some() {
                        return Err(BindError::InvalidColumn(column_name.to_string()).into());
                    }
                    got_column = Some(column_catalog);
                }
            }
            if got_column.is_none() {
                if let Some(expr) = self.context.aliases.get(column_name) {
                    return Ok(expr.clone());
                }
            }
            let column_catalog =
                got_column.ok_or_else(|| BindError::InvalidColumn(column_name.to_string()))?;
            Ok(ScalarExpression::ColumnRef(column_catalog.clone()))
        }
    }
}
