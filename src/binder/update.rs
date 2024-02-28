use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::value::DataValue;
use sqlparser::ast::{Assignment, Expr, TableFactor, TableWithJoins};
use std::slice;
use std::sync::Arc;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_update(
        &mut self,
        to: &TableWithJoins,
        selection: &Option<Expr>,
        assignments: &[Assignment],
    ) -> Result<LogicalPlan, DatabaseError> {
        // FIXME: Make it better to detect the current BindStep
        self.context.allow_default = true;
        if let TableFactor::Table { name, .. } = &to.relation {
            let table_name = Arc::new(lower_case_name(name)?);

            let mut plan = self.bind_table_ref(slice::from_ref(to))?;

            if let Some(predicate) = selection {
                plan = self.bind_where(plan, predicate)?;
            }

            let mut schema = Vec::with_capacity(assignments.len());
            let mut row = Vec::with_capacity(assignments.len());

            for Assignment { id, value } in assignments {
                let mut expression = self.bind_expr(value)?;
                expression.constant_calculation()?;

                for ident in id {
                    match self.bind_column_ref_from_identifiers(
                        slice::from_ref(ident),
                        Some(table_name.to_string()),
                    )? {
                        ScalarExpression::ColumnRef(column) => {
                            match &expression {
                                ScalarExpression::Constant(value) => {
                                    let ty = column.datatype();
                                    // Check if the value length is too long
                                    value.check_len(ty)?;

                                    if value.logical_type() != *ty {
                                        row.push(Arc::new(DataValue::clone(value).cast(ty)?));
                                    }
                                    row.push(value.clone());
                                }
                                ScalarExpression::Empty => {
                                    row.push(column.default_value().ok_or_else(|| {
                                        DatabaseError::InvalidColumn(
                                            "column does not exist default".to_string(),
                                        )
                                    })?);
                                }
                                _ => return Err(DatabaseError::UnsupportedStmt(value.to_string())),
                            }
                            schema.push(column);
                        }
                        _ => return Err(DatabaseError::InvalidColumn(ident.to_string())),
                    }
                }
            }
            self.context.allow_default = false;
            let values_plan = self.bind_values(vec![row], Arc::new(schema));

            Ok(LogicalPlan::new(
                Operator::Update(UpdateOperator { table_name }),
                vec![plan, values_plan],
            ))
        } else {
            unreachable!("only table")
        }
    }
}
