use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::execution::{build_read, Executor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct Projection {
    exprs: Vec<ScalarExpression>,
    input: LogicalPlan,
}

impl From<(ProjectOperator, LogicalPlan)> for Projection {
    fn from((ProjectOperator { exprs }, input): (ProjectOperator, LogicalPlan)) -> Self {
        Projection { exprs, input }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Projection {
    fn execute(self, transaction: &'a T) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let Projection { exprs, mut input } = self;
                let schema = input.output_schema().clone();
                let mut coroutine = build_read(input, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let mut tuple = throw!(tuple);

                    tuple.values = throw!(Self::projection(&tuple, &exprs, &schema));
                    yield Ok(tuple);
                }
            },
        )
    }
}

impl Projection {
    pub fn projection(
        tuple: &Tuple,
        exprs: &[ScalarExpression],
        schmea: &[ColumnRef],
    ) -> Result<Vec<ValueRef>, DatabaseError> {
        let mut values = Vec::with_capacity(exprs.len());

        for expr in exprs.iter() {
            values.push(expr.eval(tuple, schmea)?);
        }
        Ok(values)
    }
}
