use crate::execution::volcano::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::limit::LimitOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use futures::StreamExt;
use futures_async_stream::try_stream;
use std::cell::RefCell;

pub struct Limit {
    offset: Option<usize>,
    limit: Option<usize>,
    input: BoxedExecutor,
}

impl From<(LimitOperator, BoxedExecutor)> for Limit {
    fn from((LimitOperator { offset, limit }, input): (LimitOperator, BoxedExecutor)) -> Self {
        Limit {
            offset,
            limit,
            input,
        }
    }
}

impl<T: Transaction> Executor<T> for Limit {
    fn execute(self, _transaction: &RefCell<T>) -> BoxedExecutor {
        self._execute()
    }
}

impl Limit {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self) {
        let Limit {
            offset,
            limit,
            input,
        } = self;

        if limit.is_some() && limit.unwrap() == 0 {
            return Ok(());
        }

        let offset_val = offset.unwrap_or(0);
        let offset_limit = offset_val + limit.unwrap_or(1) - 1;

        #[for_await]
        for (i, tuple) in input.enumerate() {
            if i < offset_val {
                continue;
            } else if i > offset_limit {
                break;
            }

            yield tuple?;
        }
    }
}
