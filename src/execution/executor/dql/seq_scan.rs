use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Iter, Storage, Transaction};
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;

pub(crate) struct SeqScan {
    op: ScanOperator,
}

impl From<ScanOperator> for SeqScan {
    fn from(op: ScanOperator) -> Self {
        SeqScan { op }
    }
}

impl<S: Storage> Executor<S> for SeqScan {
    fn execute(self, storage: &S) -> BoxedExecutor {
        self._execute(storage.clone())
    }
}

impl SeqScan {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<S: Storage>(self, storage: S) {
        let ScanOperator {
            table_name,
            columns,
            limit,
            ..
        } = self.op;

        if let Some(transaction) = storage.transaction(&table_name).await {
            let mut iter = transaction.read(limit, columns)?;

            while let Some(tuple) = iter.next_tuple()? {
                yield tuple;
            }
        }
    }
}
