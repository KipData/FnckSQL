use std::slice;
use sqlparser::ast::{Expr, Ident, ObjectName};
use itertools::Itertools;
use crate::binder::{Binder, BindError, lower_case_name, split_name};
use crate::catalog::ColumnCatalog;
use crate::expression::ScalarExpression;
use crate::planner::LogicalPlan;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::Operator;
use crate::planner::operator::values::ValuesOperator;
use crate::types::value::DataValue;

impl Binder {

    // TODO: 支持Project
    pub(crate) fn bind_insert(
        &mut self,
        name: ObjectName,
        idents: &[Ident],
        rows: &Vec<Vec<Expr>>
    ) -> Result<LogicalPlan, BindError> {
        let name = lower_case_name(&name);
        let (_, table_name) = split_name(&name)?;

        if let Some(table) = self.context.catalog.get_table_by_name(table_name) {
            let mut col_catalogs = Vec::new();

            if idents.is_empty() {
                col_catalogs = table.all_columns()
                    .into_iter()
                    .map(|(_, catalog)| catalog.clone())
                    .collect_vec();
            } else {
                let bind_table_name = Some(table.name.to_string());
                for ident in idents {
                    match self.bind_column_ref_from_identifiers(
                        slice::from_ref(ident),
                        bind_table_name.as_ref()
                    )? {
                        ScalarExpression::ColumnRef(catalog) => col_catalogs.push(catalog),
                        _ => unreachable!()
                    }
                }
            }

            let rows = rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| match self.bind_expr(expr)? {
                            ScalarExpression::Constant(value) => Ok::<DataValue, BindError>(value),
                            _ => unreachable!(),
                        })
                        .try_collect()
                })
                .try_collect()?;

            let values_plan = self.bind_values(rows, col_catalogs.clone());

            Ok(LogicalPlan {
                operator: Operator::Insert(
                    InsertOperator {
                        table: table_name.to_string(),
                    }
                ),
                childrens: vec![values_plan],
            })
        } else {
            Err(BindError::InvalidTable(format!("not found table {}", table_name)))
        }
    }

    fn bind_values(
        &mut self,
        rows: Vec<Vec<DataValue>>,
        col_catalogs: Vec<ColumnCatalog>
    ) -> LogicalPlan {
        LogicalPlan {
            operator: Operator::Values(ValuesOperator {
                rows,
                col_catalogs,
            }),
            childrens: vec![],
        }
    }
}