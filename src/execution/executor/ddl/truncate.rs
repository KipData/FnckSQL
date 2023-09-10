use futures_async_stream::try_stream;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::truncate::TruncateOperator;
use crate::storage::Storage;
use crate::types::tuple::Tuple;

pub struct Truncate {
    op: TruncateOperator
}

impl From<TruncateOperator> for Truncate {
    fn from(op: TruncateOperator) -> Self {
        Truncate {
            op
        }
    }
}

impl<S: Storage> Executor<S> for Truncate {
    fn execute(self, storage: &S) -> BoxedExecutor {
        self._execute(storage.clone())
    }
}

impl Truncate {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<S: Storage>(self, storage: S) {
        let TruncateOperator { table_name } = self.op;

        storage.drop_data(&table_name).await?;
    }
}