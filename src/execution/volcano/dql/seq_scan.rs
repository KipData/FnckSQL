use crate::errors::DatabaseError;
use crate::execution::volcano::{BoxedExecutor, ReadExecutor};
use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Iter, Transaction};
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

impl<T: Transaction> ReadExecutor<T> for SeqScan {
    fn execute(self, transaction: &T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl SeqScan {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
        let ScanOperator {
            table_name,
            projection_columns: columns,
            limit,
            ..
        } = self.op;
        let mut iter = transaction.read(table_name, limit, columns)?;

        while let Some(tuple) = iter.next_tuple()? {
            yield tuple;
        }
    }
}
