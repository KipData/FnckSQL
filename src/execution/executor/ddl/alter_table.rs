use crate::execution::executor::BoxedExecutor;
use crate::types::tuple::Tuple;
use crate::{execution::ExecutorError, types::tuple_builder::TupleBuilder};
use futures_async_stream::try_stream;
use std::cell::RefCell;

use crate::{
    execution::executor::Executor, planner::operator::alter_table::AlterTableOperator,
    storage::Transaction,
};

pub struct AlterTable {
    op: AlterTableOperator,
    input: BoxedExecutor,
}

impl From<(AlterTableOperator, BoxedExecutor)> for AlterTable {
    fn from((op, input): (AlterTableOperator, BoxedExecutor)) -> Self {
        Self { op, input }
    }
}

impl<T: Transaction> Executor<T> for AlterTable {
    fn execute(self, transaction: &RefCell<T>) -> crate::execution::executor::BoxedExecutor {
        unsafe { self._execute(transaction.as_ptr().as_mut().unwrap()) }
    }
}

impl AlterTable {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let _ = transaction.alter_table(&self.op)?;
        let tuple_builder = TupleBuilder::new_result();
        let tuple = tuple_builder.push_result("ALTER TABLE SUCCESS", "1")?;

        yield tuple;
    }
}
