use futures_async_stream::try_stream;
use crate::execution::executor::BoxedExecutor;
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;

pub struct Projection { }

impl Projection {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn execute(exprs: Vec<ScalarExpression>, input: BoxedExecutor) {
        #[for_await]
        for tuple in input {
            let tuple = tuple?;

            let mut columns = Vec::with_capacity(exprs.len());
            let mut values = Vec::with_capacity(exprs.len());

            for expr in exprs.iter() {
                values.push(expr.eval_column(&tuple));
                columns.push(expr.output_column(&tuple));
            }

            yield Tuple { id: None, columns, values, };
        }
    }
}