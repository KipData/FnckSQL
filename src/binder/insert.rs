use std::slice;
use std::sync::Arc;
use sqlparser::ast::{Expr, Ident, ObjectName};
use itertools::Itertools;
use crate::binder::{Binder, BindError, lower_case_name, split_name};
use crate::catalog::ColumnRef;
use crate::expression::ScalarExpression;
use crate::planner::LogicalPlan;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::Operator;
use crate::planner::operator::values::ValuesOperator;
use crate::storage::Storage;
use crate::types::value::ValueRef;

impl Binder {
    pub(crate) fn bind_insert(
        &mut self,
        name: ObjectName,
        idents: &[Ident],
        rows: &Vec<Vec<Expr>>
    ) -> Result<LogicalPlan, BindError> {
        let name = lower_case_name(&name);
        let (_, name) = split_name(&name)?;
        let table_name = Arc::new(name.to_string());

        if let Some(table) = self.context.storage.table_catalog(&table_name) {
            let mut columns = Vec::new();

            if idents.is_empty() {
                columns = table.all_columns();
            } else {
                let bind_table_name = Some(table_name.to_string());
                for ident in idents {
                    match self.bind_column_ref_from_identifiers(
                        slice::from_ref(ident),
                        bind_table_name.as_ref()
                    )? {
                        ScalarExpression::ColumnRef(catalog) => columns.push(catalog),
                        _ => unreachable!()
                    }
                }
            }

            let rows = rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| match self.bind_expr(expr)? {
                            ScalarExpression::Constant(value) => Ok::<ValueRef, BindError>(value),
                            _ => unreachable!(),
                        })
                        .try_collect()
                })
                .try_collect()?;

            let values_plan = self.bind_values(rows, columns);

            Ok(LogicalPlan {
                operator: Operator::Insert(
                    InsertOperator {
                        table_name,
                    }
                ),
                childrens: vec![values_plan],
            })
        } else {
            Err(BindError::InvalidTable(format!("not found table {}", table_name)))
        }
    }

    pub(crate) fn bind_values(
        &mut self,
        rows: Vec<Vec<ValueRef>>,
        columns: Vec<ColumnRef>
    ) -> LogicalPlan {
        LogicalPlan {
            operator: Operator::Values(ValuesOperator {
                rows,
                columns,
            }),
            childrens: vec![],
        }
    }
}