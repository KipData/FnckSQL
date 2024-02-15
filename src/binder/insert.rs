use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::SchemaRef;
use crate::types::value::{DataValue, ValueRef};
use sqlparser::ast::{Expr, Ident, ObjectName};
use std::slice;
use std::sync::Arc;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_insert(
        &mut self,
        name: &ObjectName,
        idents: &[Ident],
        expr_rows: &Vec<Vec<Expr>>,
        is_overwrite: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name = Arc::new(lower_case_name(name)?);

        if let Some(table) = self.context.table(table_name.clone()) {
            let mut _schema_ref = None;
            let values_len = expr_rows[0].len();

            if idents.is_empty() {
                let temp_schema_ref = table.schema_ref().clone();
                if values_len > temp_schema_ref.len() {
                    return Err(DatabaseError::ValuesLenMismatch(
                        temp_schema_ref.len(),
                        values_len,
                    ));
                }
                _schema_ref = Some(temp_schema_ref);
            } else {
                let mut columns = Vec::with_capacity(idents.len());
                for ident in idents {
                    match self.bind_column_ref_from_identifiers(
                        slice::from_ref(ident),
                        Some(table_name.to_string()),
                    )? {
                        ScalarExpression::ColumnRef(catalog) => columns.push(catalog),
                        _ => unreachable!(),
                    }
                }
                if values_len != columns.len() {
                    return Err(DatabaseError::ValuesLenMismatch(columns.len(), values_len));
                }
                _schema_ref = Some(Arc::new(columns));
            }
            let schema_ref = _schema_ref.ok_or(DatabaseError::ColumnsEmpty)?;
            let mut rows = Vec::with_capacity(expr_rows.len());
            for expr_row in expr_rows {
                if expr_row.len() != values_len {
                    return Err(DatabaseError::ValuesLenNotSame());
                }
                let mut row = Vec::with_capacity(expr_row.len());

                for (i, expr) in expr_row.iter().enumerate() {
                    match &self.bind_expr(expr)? {
                        ScalarExpression::Constant(value) => {
                            // Check if the value length is too long
                            value.check_len(schema_ref[i].datatype())?;
                            let cast_value =
                                DataValue::clone(value).cast(schema_ref[i].datatype())?;
                            row.push(Arc::new(cast_value))
                        }
                        ScalarExpression::Unary { expr, op, .. } => {
                            if let ScalarExpression::Constant(value) = expr.as_ref() {
                                row.push(Arc::new(
                                    DataValue::unary_op(value, op)?
                                        .cast(schema_ref[i].datatype())?,
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
            let values_plan = self.bind_values(rows, schema_ref);

            Ok(LogicalPlan::new(
                Operator::Insert(InsertOperator {
                    table_name,
                    is_overwrite,
                }),
                vec![values_plan],
            ))
        } else {
            Err(DatabaseError::InvalidTable(format!(
                "not found table {}",
                table_name
            )))
        }
    }

    pub(crate) fn bind_values(
        &mut self,
        rows: Vec<Vec<ValueRef>>,
        schema_ref: SchemaRef,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Values(ValuesOperator { rows, schema_ref }),
            vec![],
        )
    }
}
