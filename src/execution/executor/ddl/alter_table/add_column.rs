use crate::execution::executor::BoxedExecutor;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use crate::{execution::ExecutorError, types::tuple_builder::TupleBuilder};
use futures_async_stream::try_stream;
use std::cell::RefCell;
use std::sync::Arc;

use crate::{
    execution::executor::Executor, planner::operator::alter_table::add_column::AddColumnOperator,
    storage::Transaction,
};

pub struct AddColumn {
    op: AddColumnOperator,
    input: BoxedExecutor,
}

impl From<(AddColumnOperator, BoxedExecutor)> for AddColumn {
    fn from((op, input): (AddColumnOperator, BoxedExecutor)) -> Self {
        Self { op, input }
    }
}

impl<T: Transaction> Executor<T> for AddColumn {
    fn execute(self, transaction: &RefCell<T>) -> crate::execution::executor::BoxedExecutor {
        unsafe { self._execute(transaction.as_ptr().as_mut().unwrap()) }
    }
}

impl AddColumn {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let _ = transaction.add_column(&self.op)?;

        let AddColumnOperator {
            table_name, column, ..
        } = &self.op;

        #[for_await]
        for tuple in self.input {
            let mut tuple: Tuple = tuple?;
            let is_overwrite = true;

            tuple.columns.push(Arc::new(column.clone()));
            if let Some(value) = column.default_value() {
                tuple.values.push(value);
            } else {
                tuple.values.push(Arc::new(DataValue::Null));
            }

            transaction.append(table_name, tuple, is_overwrite)?;
        }
        transaction.remove_cache(&table_name)?;

        let tuple_builder = TupleBuilder::new_result();
        let tuple = tuple_builder.push_result("ALTER TABLE SUCCESS", "1")?;

        yield tuple;
    }
}
