use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::project::ProjectOperator;
use crate::storage::Storage;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;

pub struct Projection {
    exprs: Vec<ScalarExpression>,
    input: BoxedExecutor,
}

impl From<(ProjectOperator, BoxedExecutor)> for Projection {
    fn from((ProjectOperator { columns }, input): (ProjectOperator, BoxedExecutor)) -> Self {
        Projection {
            exprs: columns,
            input,
        }
    }
}

impl<S: Storage> Executor<S> for Projection {
    fn execute(self, _: &S) -> BoxedExecutor {
        self._execute()
    }
}

impl Projection {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self) {
        let Projection { exprs, input } = self;

        #[for_await]
        for tuple in input {
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
