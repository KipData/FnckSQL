use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::value::ValueRef;
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
        if let TableFactor::Table { name, .. } = &to.relation {
            let table_name = Arc::new(lower_case_name(name)?);

            let mut plan = self.bind_table_ref(slice::from_ref(to))?;

            if let Some(predicate) = selection {
                plan = self.bind_where(plan, predicate)?;
            }

            let mut columns = Vec::with_capacity(assignments.len());
            let mut row = Vec::with_capacity(assignments.len());

            for assignment in assignments {
                let value = match self.bind_expr(&assignment.value)? {
                    ScalarExpression::Constant(value) => Ok::<ValueRef, DatabaseError>(value),
                    _ => unreachable!(),
                }?;

                for ident in &assignment.id {
                    match self.bind_column_ref_from_identifiers(
                        slice::from_ref(ident),
                        Some(table_name.to_string()),
                    )? {
                        ScalarExpression::ColumnRef(catalog) => {
                            value.check_len(catalog.datatype())?;
                            columns.push(catalog);
                            row.push(value.clone());
                        }
                        _ => unreachable!(),
                    }
                }
            }

            let values_plan = self.bind_values(vec![row], Arc::new(columns));

            Ok(LogicalPlan::new(
                Operator::Update(UpdateOperator { table_name }),
                vec![plan, values_plan],
            ))
        } else {
            unreachable!("only table")
        }
    }
}
