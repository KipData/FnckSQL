use futures_async_stream::try_stream;
use crate::execution_tp::executor::BoxedExecutor;
use crate::execution_tp::ExecutorError;
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
                values.push(expr.eval_column_tp(&tuple)?);
                columns.push(expr.output_column());
            }

            yield Tuple { id: None, columns, values, };
        }
    }
}