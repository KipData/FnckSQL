use crate::catalog::{ColumnCatalog, TableName};
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{build_read, Executor, WriteExecutor};
use crate::planner::operator::insert::InsertOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use crate::types::ColumnId;
use itertools::Itertools;
use std::collections::HashMap;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct Insert {
    table_name: TableName,
    input: LogicalPlan,
    is_overwrite: bool,
    is_mapping_by_name: bool,
}

impl From<(InsertOperator, LogicalPlan)> for Insert {
    fn from(
        (
            InsertOperator {
                table_name,
                is_overwrite,
                is_mapping_by_name,
            },
            input,
        ): (InsertOperator, LogicalPlan),
    ) -> Self {
        Insert {
            table_name,
            input,
            is_overwrite,
            is_mapping_by_name,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
enum MappingKey<'a> {
    Name(&'a str),
    Id(Option<ColumnId>),
}

impl ColumnCatalog {
    fn key(&self, is_mapping_by_name: bool) -> MappingKey {
        if is_mapping_by_name {
            MappingKey::Name(self.name())
        } else {
            MappingKey::Id(self.id())
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Insert {
    fn execute_mut(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: &'a mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let Insert {
                    table_name,
                    mut input,
                    is_overwrite,
                    is_mapping_by_name,
                } = self;

                let mut tuples = Vec::new();
                let schema = input.output_schema().clone();

                let primary_keys = schema
                    .iter()
                    .filter_map(|column| column.desc().primary().map(|i| (i, column)))
                    .sorted_by_key(|(i, _)| *i)
                    .map(|(_, col)| col.key(is_mapping_by_name))
                    .collect_vec();
                if primary_keys.is_empty() {
                    throw!(Err(DatabaseError::NotNull))
                }

                if let Some(table_catalog) =
                    throw!(transaction.table(cache.0, table_name.clone())).cloned()
                {
                    let types = table_catalog.types();
                    let mut coroutine = build_read(input, cache, transaction);

                    while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                        let Tuple { values, .. } = throw!(tuple);

                        let mut tuple_id = Vec::with_capacity(primary_keys.len());
                        let mut tuple_map = HashMap::new();
                        for (i, value) in values.into_iter().enumerate() {
                            tuple_map.insert(schema[i].key(is_mapping_by_name), value);
                        }

                        for primary_key in primary_keys.iter() {
                            tuple_id.push(throw!(tuple_map
                                .get(primary_key)
                                .cloned()
                                .ok_or(DatabaseError::NotNull)));
                        }
                        let mut values = Vec::with_capacity(table_catalog.columns_len());

                        for col in table_catalog.columns() {
                            let value = {
                                let mut value = tuple_map.remove(&col.key(is_mapping_by_name));

                                if value.is_none() {
                                    value = throw!(col.default_value());
                                }
                                value.unwrap_or_else(|| DataValue::none(col.datatype()))
                            };
                            if value.is_null() && !col.nullable() {
                                yield Err(DatabaseError::NotNull);
                                return;
                            }
                            values.push(value)
                        }
                        tuples.push(Tuple {
                            id: Some(if primary_keys.len() == 1 {
                                tuple_id.pop().unwrap()
                            } else {
                                DataValue::Tuple(Some(tuple_id))
                            }),
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
                        throw!(transaction.append_tuple(&table_name, tuple, &types, is_overwrite));
                    }
                }
                yield Ok(TupleBuilder::build_result("1".to_string()));
            },
        )
    }
}
