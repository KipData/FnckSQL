use futures::StreamExt;
use futures_async_stream::try_stream;
use crate::execution_tp::executor::BoxedExecutor;
use crate::execution_tp::ExecutorError;
use crate::types::tuple::Tuple;

pub struct Limit {}

impl Limit {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn execute(offset: Option<usize>, limit: Option<usize>, input: BoxedExecutor) {
        if limit.is_some() && limit.unwrap() == 0 {
            return Ok(());
        }

        let offset_val = offset.unwrap_or(0);
        let offset_limit = offset_val + limit.unwrap_or(1) - 1;

        #[for_await]
        for (i, tuple) in input.enumerate() {
            if i < offset_val {
                continue
            } else if i > offset_limit {
                break
            }

            yield tuple?;
        }
    }
}