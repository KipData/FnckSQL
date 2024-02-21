use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::volcano::{build_read, BoxedExecutor, WriteExecutor};
use crate::planner::operator::insert::InsertOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use futures_async_stream::try_stream;
use std::collections::HashMap;
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

impl<T: Transaction> WriteExecutor<T> for Insert {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl Insert {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let Insert {
            table_name,
            input,
            is_overwrite,
        } = self;
        let mut primary_key_id = None;
        let mut unique_values = HashMap::new();
        let mut tuple_values = Vec::new();

        if let Some(table_catalog) = transaction.table(table_name.clone()).cloned() {
            let tuple_schema_ref = table_catalog.schema_ref();

            #[for_await]
            for tuple in build_read(input, transaction) {
                let Tuple {
                    schema_ref, values, ..
                } = tuple?;

                if primary_key_id.is_none() {
                    let id = schema_ref.iter()
                        .find(|col| col.desc.is_primary)
                        .map(|col| col.id())
                        .ok_or_else(|| DatabaseError::NotNull)?;
                    primary_key_id = Some(id);
                }
                let mut tuple_map = HashMap::new();
                for (i, value) in values.into_iter().enumerate() {
                    tuple_map.insert(schema_ref[i].id(), value);
                }
                let tuple_id = tuple_map.get(&primary_key_id.unwrap()).cloned().unwrap();
                let mut values = Vec::with_capacity(table_catalog.columns_len());

                for col in table_catalog.columns() {
                    let value = tuple_map
                        .remove(&col.id())
                        .or_else(|| col.default_value())
                        .unwrap_or_else(|| Arc::new(DataValue::none(col.datatype())));

                    if col.desc.is_unique && !value.is_null() {
                        unique_values
                            .entry(col.id())
                            .or_insert_with(Vec::new)
                            .push((tuple_id.clone(), value.clone()))
                    }
                    if value.is_null() && !col.nullable {
                        return Err(DatabaseError::NotNull);
                    }
                    values.push(value)
                }
                tuple_values.push((tuple_id, values));
            }
            let tuple_builder = TupleBuilder::new(tuple_schema_ref);

            // Unique Index
            for (col_id, values) in unique_values {
                if let Some(index_meta) = table_catalog.get_unique_index(&col_id.unwrap()) {
                    for (tuple_id, value) in values {
                        let index = Index {
                            id: index_meta.id,
                            column_values: vec![value],
                        };

                        transaction.add_index(&table_name, index, vec![tuple_id], true)?;
                    }
                }
            }
            for (tuple_id, values) in tuple_values {
                let tuple = tuple_builder.build(Some(tuple_id), values)?;

                transaction.append(&table_name, tuple, is_overwrite)?;
            }
        }
    }
}
