use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::drop_table::DropTableOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use std::cell::RefCell;

pub struct DropTable {
    op: DropTableOperator,
}

impl From<DropTableOperator> for DropTable {
    fn from(op: DropTableOperator) -> Self {
        DropTable { op }
    }
}

impl<T: Transaction> Executor<T> for DropTable {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor {
        unsafe { self._execute(transaction.as_ptr().as_mut().unwrap()) }
    }
}

impl DropTable {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let DropTableOperator { table_name } = self.op;

        transaction.drop_table(&table_name)?;
    }
}
