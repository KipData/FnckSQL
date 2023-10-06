use crate::execution::executor::{BoxedExecutor, Executor};
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
}

impl From<LimitOperator> for Limit {
    fn from(LimitOperator { offset, limit }: LimitOperator) -> Limit {
        Limit {
            offset: Some(offset),
            limit: Some(limit),
        }
    }
}

impl<T: Transaction> Executor<T> for Limit {
    fn execute(self, inputs: Vec<BoxedExecutor>, _transaction: &RefCell<T>) -> BoxedExecutor {
        self._execute(inputs)
    }
}

impl Limit {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self, mut inputs: Vec<BoxedExecutor>) {
        let Limit { offset, limit } = self;

        if limit.is_some() && limit.unwrap() == 0 {
            return Ok(());
        }

        let offset_val = offset.unwrap_or(0);
        let offset_limit = offset_val + limit.unwrap_or(1) - 1;

        #[for_await]
        for (i, tuple) in inputs.remove(0).enumerate() {
            if i < offset_val {
                continue;
            } else if i > offset_limit {
                break;
            }

            yield tuple?;
        }
    }
}
