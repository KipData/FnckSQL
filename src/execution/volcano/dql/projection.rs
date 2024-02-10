use crate::errors::DatabaseError;
use crate::execution::volcano::{build_read, BoxedExecutor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use std::sync::Arc;

pub struct Projection {
    exprs: Vec<ScalarExpression>,
    input: LogicalPlan,
}

impl From<(ProjectOperator, LogicalPlan)> for Projection {
    fn from((ProjectOperator { exprs }, input): (ProjectOperator, LogicalPlan)) -> Self {
        Projection { exprs, input }
    }
}

impl<T: Transaction> ReadExecutor<T> for Projection {
    fn execute(self, transaction: &T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl Projection {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
        let Projection { exprs, input } = self;
        let mut schema_ref = None;

        #[for_await]
        for tuple in build_read(input, transaction) {
            let mut tuple = tuple?;
            let mut values = Vec::with_capacity(exprs.len());
            let schema_ref = schema_ref.get_or_insert_with(|| {
                let mut columns = Vec::with_capacity(exprs.len());

                for expr in exprs.iter() {
                    columns.push(expr.output_column());
                }
                Arc::new(columns)
            });

            for expr in exprs.iter() {
                values.push(expr.eval(&tuple)?);
            }
            tuple.schema_ref = schema_ref.clone();
            tuple.values = values;

            yield tuple;
        }
    }
}
