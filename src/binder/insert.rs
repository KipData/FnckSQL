use crate::binder::{lower_case_name, split_name, BindError, Binder};
use crate::catalog::ColumnRef;
use crate::expression::value_compute::unary_op;
use crate::expression::ScalarExpression;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::value::{DataValue, ValueRef};
use sqlparser::ast::{Expr, Ident, ObjectName};
use std::slice;
use std::sync::Arc;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_insert(
        &mut self,
        name: ObjectName,
        idents: &[Ident],
        expr_rows: &Vec<Vec<Expr>>,
        is_overwrite: bool,
    ) -> Result<LogicalPlan, BindError> {
        let name = lower_case_name(&name);
        let name = split_name(&name)?;
        let table_name = Arc::new(name.to_string());

        if let Some(table) = self.context.table(table_name.clone()) {
            let mut columns = Vec::new();
            let values_len = expr_rows[0].len();

            if idents.is_empty() {
                columns = table.all_columns();
                if values_len > columns.len() {
                    return Err(BindError::ValuesLenMismatch(columns.len(), values_len));
                }
            } else {
                let bind_table_name = Some(table_name.to_string());
                for ident in idents {
                    match self.bind_column_ref_from_identifiers(
                        slice::from_ref(ident),
                        bind_table_name.as_ref(),
                    )? {
                        ScalarExpression::ColumnRef(catalog) => columns.push(catalog),
                        _ => unreachable!(),
                    }
                }
                if values_len != columns.len() {
                    return Err(BindError::ValuesLenMismatch(columns.len(), values_len));
                }
            }
            let mut rows = Vec::with_capacity(expr_rows.len());
            for expr_row in expr_rows {
                if expr_row.len() != values_len {
                    return Err(BindError::ValuesLenNotSame());
                }
                let mut row = Vec::with_capacity(expr_row.len());

                for (i, expr) in expr_row.iter().enumerate() {
                    match &self.bind_expr(expr)? {
                        ScalarExpression::Constant(value) => {
                            // Check if the value length is too long
                            value.check_len(columns[i].datatype())?;
                            let cast_value = DataValue::clone(value).cast(columns[i].datatype())?;
                            row.push(Arc::new(cast_value))
                        }
                        ScalarExpression::Unary { expr, op, .. } => {
                            if let ScalarExpression::Constant(value) = expr.as_ref() {
                                row.push(Arc::new(
                                    unary_op(value, op)?.cast(columns[i].datatype())?,
                                ))
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
                operator: Operator::Insert(InsertOperator {
                    table_name,
                    is_overwrite,
                }),
                childrens: vec![values_plan],
            })
        } else {
            Err(BindError::InvalidTable(format!(
                "not found table {}",
                table_name
            )))
        }
    }

    pub(crate) fn bind_values(
        &mut self,
        rows: Vec<Vec<ValueRef>>,
        columns: Vec<ColumnRef>,
    ) -> LogicalPlan {
        LogicalPlan {
            operator: Operator::Values(ValuesOperator { rows, columns }),
            childrens: vec![],
        }
    }
}
