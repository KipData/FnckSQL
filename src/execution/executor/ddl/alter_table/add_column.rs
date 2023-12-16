use crate::execution::executor::BoxedExecutor;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use crate::{execution::ExecutorError, types::tuple_builder::TupleBuilder};
use futures_async_stream::try_stream;
use std::cell::RefCell;
use std::sync::Arc;

use crate::types::index::Index;
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
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor {
        unsafe { self._execute(transaction.as_ptr().as_mut().unwrap()) }
    }
}

impl AddColumn {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let AddColumnOperator {
            table_name,
            column,
            if_not_exists,
        } = &self.op;
        let mut unique_values = column.desc().is_unique.then(|| Vec::new());

        #[for_await]
        for tuple in self.input {
            let mut tuple: Tuple = tuple?;

            tuple.columns.push(Arc::new(column.clone()));
            if let Some(value) = column.default_value() {
                if let Some(unique_values) = &mut unique_values {
                    unique_values.push((tuple.id.clone().unwrap(), value.clone()));
                }
                tuple.values.push(value);
            } else {
                tuple.values.push(Arc::new(DataValue::Null));
            }
            transaction.append(table_name, tuple, true)?;
        }
        let col_id = transaction.add_column(table_name, column, *if_not_exists)?;

        // Unique Index
        if let (Some(unique_values), Some(unique_meta)) = (
            unique_values,
            transaction
                .table(table_name.clone())
                .and_then(|table| table.get_unique_index(&col_id))
                .cloned(),
        ) {
            for (tuple_id, value) in unique_values {
                let index = Index {
                    id: unique_meta.id,
                    column_values: vec![value],
                };
                transaction.add_index(&table_name, index, vec![tuple_id], true)?;
            }
        }

        let tuple_builder = TupleBuilder::new_result();
        let tuple = tuple_builder.push_result("ALTER TABLE SUCCESS", "1")?;

        yield tuple;
    }
}
