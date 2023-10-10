use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Iter, Transaction};
use crate::types::errors::TypeError;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use std::cell::RefCell;

pub(crate) struct IndexScan {
    op: ScanOperator,
}

impl From<ScanOperator> for IndexScan {
    fn from(op: ScanOperator) -> Self {
        IndexScan { op }
    }
}

impl<T: Transaction> Executor<T> for IndexScan {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor {
        unsafe { self._execute(transaction.as_ptr().as_ref().unwrap()) }
    }
}

impl IndexScan {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
        let ScanOperator {
            table_name,
            columns,
            limit,
            index_by,
            ..
        } = self.op;
        let (index_meta, binaries) = index_by.ok_or(TypeError::InvalidType)?;
        let mut iter =
            transaction.read_by_index(&table_name, limit, columns, index_meta, binaries)?;

        while let Some(tuple) = iter.next_tuple()? {
            yield tuple;
        }
    }
}
