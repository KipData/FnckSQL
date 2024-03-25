use crate::errors::DatabaseError;
use crate::execution::volcano::{build_read, BoxedExecutor, WriteExecutor};
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use futures_async_stream::try_stream;
use std::slice;
use std::sync::Arc;

use crate::planner::LogicalPlan;
use crate::types::index::{Index, IndexType};
use crate::{planner::operator::alter_table::add_column::AddColumnOperator, storage::Transaction};

pub struct AddColumn {
    op: AddColumnOperator,
    input: LogicalPlan,
}

impl From<(AddColumnOperator, LogicalPlan)> for AddColumn {
    fn from((op, input): (AddColumnOperator, LogicalPlan)) -> Self {
        Self { op, input }
    }
}

impl<T: Transaction> WriteExecutor<T> for AddColumn {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl AddColumn {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    async fn _execute<T: Transaction>(mut self, transaction: &mut T) {
        let AddColumnOperator {
            table_name,
            column,
            if_not_exists,
        } = &self.op;
        let mut unique_values = column.desc().is_unique.then(Vec::new);
        let mut tuples = Vec::new();
        let schema = self.input.output_schema();
        let mut types = Vec::with_capacity(schema.len() + 1);

        for column_ref in schema.iter() {
            types.push(*column_ref.datatype());
        }
        types.push(*column.datatype());

        #[for_await]
        for tuple in build_read(self.input, transaction) {
            let mut tuple: Tuple = tuple?;

            if let Some(value) = column.default_value()? {
                if let Some(unique_values) = &mut unique_values {
                    unique_values.push((tuple.id.clone().unwrap(), value.clone()));
                }
                tuple.values.push(value);
            } else {
                tuple.values.push(Arc::new(DataValue::Null));
            }
            tuples.push(tuple);
        }
        for tuple in tuples {
            transaction.append(table_name, tuple, &types, true)?;
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
                let index = Index::new(unique_meta.id, slice::from_ref(&value), IndexType::Unique);
                transaction.add_index(table_name, index, &tuple_id)?;
            }
        }

        yield TupleBuilder::build_result("1".to_string());
    }
}
