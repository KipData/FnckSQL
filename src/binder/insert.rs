use std::slice;
use std::sync::Arc;
use sqlparser::ast::{Expr, Ident, ObjectName};
use crate::binder::{Binder, BindError, lower_case_name, split_name};
use crate::catalog::ColumnRef;
use crate::expression::ScalarExpression;
use crate::expression::value_compute::unary_op;
use crate::planner::LogicalPlan;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::Operator;
use crate::planner::operator::values::ValuesOperator;
use crate::storage::Storage;
use crate::types::value::{DataValue, ValueRef};

impl<S: Storage> Binder<S> {
    pub(crate) async fn bind_insert(
        &mut self,
        name: ObjectName,
        idents: &[Ident],
        expr_rows: &Vec<Vec<Expr>>,
        is_overwrite: bool
    ) -> Result<LogicalPlan, BindError> {
        let name = lower_case_name(&name);
        let (_, name) = split_name(&name)?;
        let table_name = Arc::new(name.to_string());

        if let Some(table) = self.context.storage.table_catalog(&table_name).await {
            let mut columns = Vec::new();

            if idents.is_empty() {
                columns = table.all_columns();
            } else {
                let bind_table_name = Some(table_name.to_string());
                for ident in idents {
                    match self.bind_column_ref_from_identifiers(
                        slice::from_ref(ident),
                        bind_table_name.as_ref()
                    ).await? {
                        ScalarExpression::ColumnRef(catalog) => columns.push(catalog),
                        _ => unreachable!()
                    }
                }
            }
            let mut rows = Vec::with_capacity(expr_rows.len());

            for expr_row in expr_rows {
                let mut row = Vec::with_capacity(expr_row.len());

                for (i, expr) in expr_row.into_iter().enumerate() {
                    match &self.bind_expr(expr).await? {
                        ScalarExpression::Constant(value) => {
                            let cast_value = DataValue::clone(value)
                                .cast(columns[i].datatype())?;

                            row.push(Arc::new(cast_value))
                        },
                        ScalarExpression::Unary { expr, op, .. } => {
                            if let ScalarExpression::Constant(value) = expr.as_ref() {
                                row.push(Arc::new(unary_op(value, op)?))
                            } else {
                                unreachable!()
                            }
                        }
                        _ => unreachable!(),
                    }
                }

                rows.push(row);
            }
            let values_plan = self.bind_values(rows, columns);

            Ok(LogicalPlan {
                operator: Operator::Insert(
                    InsertOperator {
                        table_name,
                        is_overwrite,
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