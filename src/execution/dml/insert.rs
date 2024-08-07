use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{build_read, Executor, WriteExecutor};
use crate::planner::operator::insert::InsertOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::throw;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use std::collections::HashMap;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;
use std::sync::Arc;

pub struct Insert {
    table_name: TableName,
    input: LogicalPlan,
    is_overwrite: bool,
}

impl From<(InsertOperator, LogicalPlan)> for Insert {
    fn from(
        (
            InsertOperator {
                table_name,
                is_overwrite,
            },
            input,
        ): (InsertOperator, LogicalPlan),
    ) -> Self {
        Insert {
            table_name,
            input,
            is_overwrite,
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Insert {
    fn execute_mut(self, transaction: &'a mut T) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let Insert {
                    table_name,
                    mut input,
                    is_overwrite,
                } = self;

                let mut tuples = Vec::new();
                let schema = input.output_schema().clone();

                let pk_index = throw!(schema
                    .iter()
                    .find(|col| col.desc.is_primary)
                    .map(|col| col.id())
                    .ok_or_else(|| DatabaseError::NotNull));

                if let Some(table_catalog) = transaction.table(table_name.clone()).cloned() {
                    let types = table_catalog.types();
                    let mut coroutine = build_read(input, transaction);

                    while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                        let Tuple { values, .. } = throw!(tuple);

                        let mut tuple_map = HashMap::new();
                        for (i, value) in values.into_iter().enumerate() {
                            tuple_map.insert(schema[i].id(), value);
                        }
                        let tuple_id = throw!(tuple_map
                            .get(&pk_index)
                            .cloned()
                            .ok_or(DatabaseError::NotNull));
                        let mut values = Vec::with_capacity(table_catalog.columns_len());

                        for col in table_catalog.columns() {
                            let value = {
                                let mut value = tuple_map.remove(&col.id());

                                if value.is_none() {
                                    value = throw!(col.default_value());
                                }
                                value.unwrap_or_else(|| Arc::new(DataValue::none(col.datatype())))
                            };
                            if value.is_null() && !col.nullable {
                                yield Err(DatabaseError::NotNull);
                                return;
                            }
                            values.push(value)
                        }
                        tuples.push(Tuple {
                            id: Some(tuple_id),
                            values,
                        });
                    }
                    drop(coroutine);
                    for index_meta in table_catalog.indexes() {
                        let exprs = throw!(index_meta.column_exprs(&table_catalog));

                        for tuple in tuples.iter() {
                            let values = throw!(Projection::projection(tuple, &exprs, &schema));
                            let index = Index::new(index_meta.id, &values, index_meta.ty);

                            throw!(transaction.add_index(
                                &table_name,
                                index,
                                tuple.id.as_ref().unwrap()
                            ));
                        }
                    }
                    for tuple in tuples {
                        throw!(transaction.append(&table_name, tuple, &types, is_overwrite));
                    }
                }
                yield Ok(TupleBuilder::build_result("1".to_string()));
            },
        )
    }
}
