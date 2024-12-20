use crate::errors::DatabaseError;
use crate::execution::{build_read, Executor, WriteExecutor};
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct DropColumn {
    op: DropColumnOperator,
    input: LogicalPlan,
}

impl From<(DropColumnOperator, LogicalPlan)> for DropColumn {
    fn from((op, input): (DropColumnOperator, LogicalPlan)) -> Self {
        Self { op, input }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for DropColumn {
    fn execute_mut(
        mut self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
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
                    .map(|(i, column)| (i, column.desc().is_primary()))
                {
                    if is_primary {
                        throw!(Err(DatabaseError::InvalidColumn(
                            "drop of primary key column is not allowed.".to_owned(),
                        )));
                    }
                    let mut tuples = Vec::new();
                    let mut types = Vec::with_capacity(tuple_columns.len() - 1);

                    for (i, column_ref) in tuple_columns.iter().enumerate() {
                        if i == column_index {
                            continue;
                        }
                        types.push(column_ref.datatype().clone());
                    }
                    let mut coroutine = build_read(self.input, cache, transaction);

                    while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                        let mut tuple: Tuple = throw!(tuple);
                        let _ = tuple.values.remove(column_index);

                        tuples.push(tuple);
                    }
                    drop(coroutine);
                    for tuple in tuples {
                        throw!(unsafe { &mut (*transaction) }.append_tuple(
                            &table_name,
                            tuple,
                            &types,
                            true
                        ));
                    }
                    throw!(unsafe { &mut (*transaction) }.drop_column(
                        cache.0,
                        cache.2,
                        &table_name,
                        &column_name
                    ));

                    yield Ok(TupleBuilder::build_result("1".to_string()));
                } else if if_exists {
                    return;
                } else {
                    yield Err(DatabaseError::ColumnNotFound(column_name));
                }
            },
        )
    }
}
