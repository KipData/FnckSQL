use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::project::ProjectOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use std::cell::RefCell;

pub struct Projection {
    exprs: Vec<ScalarExpression>,
}

impl From<ProjectOperator> for Projection {
    fn from(ProjectOperator { columns }: ProjectOperator) -> Projection {
        Projection { exprs: columns }
    }
}

impl<T: Transaction> Executor<T> for Projection {
    fn execute<'a>(self, inputs: Vec<BoxedExecutor>, _transaction: &RefCell<T>) -> BoxedExecutor {
        self._execute(inputs)
    }
}

impl Projection {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self, mut inputs: Vec<BoxedExecutor>) {
        let Projection { exprs } = self;

        #[for_await]
        for tuple in inputs.remove(0) {
            let tuple = tuple?;

            let mut columns = Vec::with_capacity(exprs.len());
            let mut values = Vec::with_capacity(exprs.len());

            for expr in exprs.iter() {
                values.push(expr.eval_column(&tuple)?);
                columns.push(expr.output_columns(&tuple));
            }

            yield Tuple {
                id: None,
                columns,
                values,
            };
        }
    }
}
