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

impl<'a, 'b, T: Transaction> Binder<'a, 'b, T> {
    pub(crate) fn bind_insert(
        &mut self,
        name: &ObjectName,
        idents: &[Ident],
        expr_rows: &Vec<Vec<Expr>>,
        is_overwrite: bool,
        is_mapping_by_name: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        // FIXME: Make it better to detect the current BindStep
        self.context.allow_default = true;
        let table_name = Arc::new(lower_case_name(name)?);

        let table = self
            .context
            .table_and_bind(table_name.clone(), None, None)?;
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
                    _ => return Err(DatabaseError::UnsupportedStmt(ident.to_string())),
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
                return Err(DatabaseError::ValuesLenMismatch(expr_row.len(), values_len));
            }
            let mut row = Vec::with_capacity(expr_row.len());

            for (i, expr) in expr_row.iter().enumerate() {
                let mut expression = self.bind_expr(expr)?;

                expression.constant_calculation()?;
                match expression {
                    ScalarExpression::Constant(mut value) => {
                        let ty = schema_ref[i].datatype();
                        // Check if the value length is too long
                        value.check_len(ty)?;

                        if value.logical_type() != *ty {
                            value = Arc::new(DataValue::clone(&value).cast(ty)?);
                        }
                        row.push(value);
                    }
                    ScalarExpression::Empty => {
                        let default_value = schema_ref[i]
                            .default_value()?
                            .ok_or(DatabaseError::DefaultNotExist)?;
                        row.push(default_value);
                    }
                    _ => return Err(DatabaseError::UnsupportedStmt(expr.to_string())),
                }
            }
            rows.push(row);
        }
        self.context.allow_default = false;
        let values_plan = self.bind_values(rows, schema_ref);

        Ok(LogicalPlan::new(
            Operator::Insert(InsertOperator {
                table_name,
                is_overwrite,
                is_mapping_by_name,
            }),
            vec![values_plan],
        ))
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
