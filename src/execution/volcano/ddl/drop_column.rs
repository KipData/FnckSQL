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
    async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let DropColumnOperator {
            table_name,
            column_name,
            if_exists,
        } = &self.op;
        let mut option_column_i = None;
        let mut tuples = Vec::new();

        #[for_await]
        for tuple in build_read(self.input, transaction) {
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
                        Err(DatabaseError::InvalidColumn(
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
                .ok_or_else(|| DatabaseError::InvalidColumn("not found column".to_string()))?;

            let _ = tuple.columns.remove(column_i);
            let _ = tuple.values.remove(column_i);

            tuples.push(tuple);
        }
        for tuple in tuples {
            transaction.append(table_name, tuple, true)?;
        }
        transaction.drop_column(table_name, column_name, *if_exists)?;

        yield TupleBuilder::build_result("ALTER TABLE SUCCESS".to_string(), "1".to_string())?;
    }
}
