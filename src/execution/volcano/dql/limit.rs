use crate::execution::volcano::{build_read, BoxedExecutor, ReadExecutor};
use crate::execution::ExecutorError;
use crate::planner::operator::limit::LimitOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use futures::StreamExt;
use futures_async_stream::try_stream;

pub struct Limit {
    offset: Option<usize>,
    limit: Option<usize>,
    input: LogicalPlan,
}

impl From<(LimitOperator, LogicalPlan)> for Limit {
    fn from((LimitOperator { offset, limit }, input): (LimitOperator, LogicalPlan)) -> Self {
        Limit {
            offset,
            limit,
            input,
        }
    }
}

impl<T: Transaction> ReadExecutor<T> for Limit {
    fn execute(self, transaction: &T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl Limit {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
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
        for (i, tuple) in build_read(input, transaction).enumerate() {
            if i < offset_val {
                continue;
            } else if i > offset_limit {
                break;
            }

            yield tuple?;
        }
    }
}
