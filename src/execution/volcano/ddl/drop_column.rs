use crate::binder::BindError;
use crate::execution::volcano::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use futures_async_stream::try_stream;
use std::cell::RefCell;

pub struct DropColumn {
    op: DropColumnOperator,
    input: BoxedExecutor,
}

impl From<(DropColumnOperator, BoxedExecutor)> for DropColumn {
    fn from((op, input): (DropColumnOperator, BoxedExecutor)) -> Self {
        Self { op, input }
    }
}

impl<T: Transaction> Executor<T> for DropColumn {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor {
        unsafe { self._execute(transaction.as_ptr().as_mut().unwrap()) }
    }
}

impl DropColumn {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let DropColumnOperator {
            table_name,
            column_name,
            if_exists,
        } = &self.op;
        let mut option_column_i = None;

        #[for_await]
        for tuple in self.input {
            let mut tuple: Tuple = tuple?;

            if option_column_i.is_none() {
                if let Some((column_index, is_primary)) = tuple
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.name() == column_name)
                    .map(|(i, column)| (i, column.desc.is_primary))
                {
                    if is_primary {
                        Err(BindError::InvalidColumn(
                            "drop of primary key column is not allowed.".to_owned(),
                        ))?;
                    }
                    option_column_i = Some(column_index);
                }
            }
            if option_column_i.is_none() && *if_exists {
                return Ok(());
            }
            let column_i = option_column_i
                .ok_or_else(|| BindError::InvalidColumn("not found column".to_string()))?;

            let _ = tuple.columns.remove(column_i);
            let _ = tuple.values.remove(column_i);

            transaction.append(table_name, tuple, true)?;
        }
        transaction.drop_column(table_name, column_name, *if_exists)?;

        yield TupleBuilder::build_result("ALTER TABLE SUCCESS".to_string(), "1".to_string())?;
    }
}
