use crate::execution::volcano::{BoxedExecutor, WriteExecutor};
use crate::execution::ExecutorError;
use crate::planner::operator::truncate::TruncateOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;

pub struct Truncate {
    op: TruncateOperator,
}

impl From<TruncateOperator> for Truncate {
    fn from(op: TruncateOperator) -> Self {
        Truncate { op }
    }
}

impl<T: Transaction> WriteExecutor<T> for Truncate {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl Truncate {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let TruncateOperator { table_name } = self.op;

        transaction.drop_data(&table_name)?;
    }
}
