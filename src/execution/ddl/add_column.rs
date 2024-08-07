use crate::execution::{build_read, Executor, WriteExecutor};
use crate::planner::LogicalPlan;
use crate::types::index::{Index, IndexType};
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use crate::{
    planner::operator::alter_table::add_column::AddColumnOperator, storage::Transaction, throw,
};
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;
use std::slice;
use std::sync::Arc;

pub struct AddColumn {
    op: AddColumnOperator,
    input: LogicalPlan,
}

impl From<(AddColumnOperator, LogicalPlan)> for AddColumn {
    fn from((op, input): (AddColumnOperator, LogicalPlan)) -> Self {
        Self { op, input }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for AddColumn {
    fn execute_mut(mut self, transaction: &'a mut T) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
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

                let mut coroutine = build_read(self.input, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let mut tuple: Tuple = throw!(tuple);

                    if let Some(value) = throw!(column.default_value()) {
                        if let Some(unique_values) = &mut unique_values {
                            unique_values.push((tuple.id.clone().unwrap(), value.clone()));
                        }
                        tuple.values.push(value);
                    } else {
                        tuple.values.push(Arc::new(DataValue::Null));
                    }
                    tuples.push(tuple);
                }
                drop(coroutine);

                for tuple in tuples {
                    throw!(transaction.append(table_name, tuple, &types, true));
                }
                let col_id = throw!(transaction.add_column(table_name, column, *if_not_exists));

                // Unique Index
                if let (Some(unique_values), Some(unique_meta)) = (
                    unique_values,
                    transaction
                        .table(table_name.clone())
                        .and_then(|table| table.get_unique_index(&col_id))
                        .cloned(),
                ) {
                    for (tuple_id, value) in unique_values {
                        let index =
                            Index::new(unique_meta.id, slice::from_ref(&value), IndexType::Unique);
                        throw!(transaction.add_index(table_name, index, &tuple_id));
                    }
                }

                yield Ok(TupleBuilder::build_result("1".to_string()));
            },
        )
    }
}
