use crate::errors::DatabaseError;
use crate::execution::volcano::{BoxedExecutor, ReadExecutor};
use crate::expression::simplify::ConstantBinary;
use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Iter, Transaction};
use crate::types::index::IndexMetaRef;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;

pub(crate) struct IndexScan {
    op: ScanOperator,
    index_by: IndexMetaRef,
    ranges: Vec<ConstantBinary>,
}

impl From<(ScanOperator, IndexMetaRef, Vec<ConstantBinary>)> for IndexScan {
    fn from((op, index_by, ranges): (ScanOperator, IndexMetaRef, Vec<ConstantBinary>)) -> Self {
        IndexScan {
            op,
            index_by,
            ranges,
        }
    }
}

impl<T: Transaction> ReadExecutor<T> for IndexScan {
    fn execute(self, transaction: &T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl IndexScan {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
        let ScanOperator {
            table_name,
            columns,
            limit,
            ..
        } = self.op;
        let mut iter =
            transaction.read_by_index(table_name, limit, columns, self.index_by, self.ranges)?;

        while let Some(tuple) = iter.next_tuple()? {
            yield tuple;
        }
    }
}
