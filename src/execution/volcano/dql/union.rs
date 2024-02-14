use crate::errors::DatabaseError;
use crate::execution::volcano::{build_read, BoxedExecutor, ReadExecutor};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;

pub struct Union {
    left_input: LogicalPlan,
    right_input: LogicalPlan,
}

impl From<(LogicalPlan, LogicalPlan)> for Union {
    fn from((left_input, right_input): (LogicalPlan, LogicalPlan)) -> Self {
        Union {
            left_input,
            right_input,
        }
    }
}

impl<T: Transaction> ReadExecutor<T> for Union {
    fn execute(self, transaction: &T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl Union {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
        let Union {
            left_input,
            right_input,
        } = self;

        #[for_await]
        for tuple in build_read(left_input, transaction) {
            yield tuple?;
        }
        #[for_await]
        for tuple in build_read(right_input, transaction) {
            yield tuple?;
        }
    }
}
