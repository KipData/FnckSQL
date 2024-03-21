use crate::errors::DatabaseError;
use crate::execution::volcano::{build_read, BoxedExecutor, WriteExecutor};
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use futures_async_stream::try_stream;

pub struct DropColumn {
    op: DropColumnOperator,
    input: LogicalPlan,
}

impl From<(DropColumnOperator, LogicalPlan)> for DropColumn {
    fn from((op, input): (DropColumnOperator, LogicalPlan)) -> Self {
        Self { op, input }
    }
}

impl<T: Transaction> WriteExecutor<T> for DropColumn {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl DropColumn {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    async fn _execute<T: Transaction>(mut self, transaction: &mut T) {
        let DropColumnOperator {
            table_name,
            column_name,
            if_exists,
        } = self.op;
        let tuple_columns = self.input.output_schema();

        if let Some((column_index, is_primary)) = tuple_columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name() == column_name)
            .map(|(i, column)| (i, column.desc.is_primary))
        {
            if is_primary {
                Err(DatabaseError::InvalidColumn(
                    "drop of primary key column is not allowed.".to_owned(),
                ))?;
            }
            let mut tuples = Vec::new();
            let mut types = Vec::with_capacity(tuple_columns.len() - 1);

            for (i, column_ref) in tuple_columns.iter().enumerate() {
                if i == column_index {
                    continue;
                }
                types.push(*column_ref.datatype());
            }
            #[for_await]
            for tuple in build_read(self.input, transaction) {
                let mut tuple: Tuple = tuple?;
                let _ = tuple.values.remove(column_index);

                tuples.push(tuple);
            }
            for tuple in tuples {
                transaction.append(&table_name, tuple, &types, true)?;
            }
            transaction.drop_column(&table_name, &column_name)?;

            yield TupleBuilder::build_result("1".to_string());
        } else if if_exists {
            return Ok(());
        } else {
            return Err(DatabaseError::NotFound("drop column", column_name));
        }
    }
}
