use futures_async_stream::try_stream;
use crate::execution_tp::executor::BoxedExecutor;
use crate::execution_tp::ExecutorError;
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;

pub struct Filter { }

impl Filter {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn execute(predicate: ScalarExpression, input: BoxedExecutor) {
        #[for_await]
        for tuple in input {
            let tuple = tuple?;
            if let DataValue::Boolean(option) = predicate.eval_column_tp(&tuple) {
                if let Some(true) = option{
                    yield tuple;
                } else {
                    continue
                }
            } else {
                unreachable!("only bool");
            }
        }
    }
}