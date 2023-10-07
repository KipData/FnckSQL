use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::truncate::TruncateOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use std::cell::RefCell;

pub struct Truncate {
    op: TruncateOperator,
}

impl From<TruncateOperator> for Truncate {
    fn from(op: TruncateOperator) -> Self {
        Truncate { op }
    }
}

impl<T: Transaction> Executor<T> for Truncate {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor {
        unsafe { self._execute(transaction.as_ptr().as_mut().unwrap()) }
    }
}

impl Truncate {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let TruncateOperator { table_name } = self.op;

        transaction.drop_data(&table_name)?;
    }
}
