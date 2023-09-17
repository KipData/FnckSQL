use std::slice;
use std::sync::Arc;
use sqlparser::ast::{Assignment, Expr, TableFactor, TableWithJoins};
use crate::binder::{Binder, BindError, lower_case_name, split_name};
use crate::expression::ScalarExpression;
use crate::planner::LogicalPlan;
use crate::planner::operator::Operator;
use crate::planner::operator::update::UpdateOperator;
use crate::storage::Storage;
use crate::types::value::ValueRef;

impl<S: Storage> Binder<S> {
    pub(crate) async fn bind_update(
        &mut self,
        to: &TableWithJoins,
        selection: &Option<Expr>,
        assignments: &[Assignment]
    ) -> Result<LogicalPlan, BindError> {
        if let TableFactor::Table { name, .. } = &to.relation {
            let name = lower_case_name(&name);
            let (_, name) = split_name(&name)?;
            let table_name = Arc::new(name.to_string());

            let mut plan = self.bind_table_ref(slice::from_ref(to)).await?;

            if let Some(predicate) = selection {
                plan = self.bind_where(plan, predicate).await?;
            }

            let bind_table_name = Some(table_name.to_string());

            let mut columns = Vec::with_capacity(assignments.len());
            let mut row = Vec::with_capacity(assignments.len());

            for assignment in assignments {
                let value = match self.bind_expr(&assignment.value).await? {
                    ScalarExpression::Constant(value) => Ok::<ValueRef, BindError>(value),
                    _ => unreachable!(),
                }?;

                for ident in &assignment.id {
                    match self.bind_column_ref_from_identifiers(
                        slice::from_ref(&ident),
                        bind_table_name.as_ref()
                    ).await? {
                        ScalarExpression::ColumnRef(catalog) => {
                            value.check_length(catalog.datatype())?;
                            columns.push(catalog);
                            row.push(value.clone());
                        },
                        _ => unreachable!()
                    }
                }
            }

            let values_plan = self.bind_values(vec![row], columns);

            Ok(LogicalPlan {
                operator: Operator::Update(
                    UpdateOperator {
                        table_name
                    }
                ),
                childrens: vec![plan, values_plan],
            })
        } else {
            unreachable!("only table")
        }
    }
}
