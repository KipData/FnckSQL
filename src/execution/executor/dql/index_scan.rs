use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Iter, Storage, Transaction};
use crate::types::errors::TypeError;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;

pub(crate) struct IndexScan {
    op: ScanOperator,
}

impl From<ScanOperator> for IndexScan {
    fn from(op: ScanOperator) -> Self {
        IndexScan { op }
    }
}

impl<S: Storage> Executor<S> for IndexScan {
    fn execute(self, storage: &S) -> BoxedExecutor {
        self._execute(storage.clone())
    }
}

impl IndexScan {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<S: Storage>(self, storage: S) {
        let ScanOperator {
            table_name,
            columns,
            limit,
            index_by,
            ..
        } = self.op;
        let (index_meta, binaries) = index_by.ok_or(TypeError::InvalidType)?;

        if let Some(transaction) = storage.transaction(&table_name).await {
            let mut iter = transaction.read_by_index(limit, columns, index_meta, binaries)?;

            while let Some(tuple) = iter.next_tuple()? {
                yield tuple;
            }
        }
    }
}
