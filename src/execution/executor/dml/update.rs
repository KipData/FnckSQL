use std::collections::HashMap;
use futures_async_stream::try_stream;
use crate::catalog::CatalogError;
use crate::execution::executor::BoxedExecutor;
use crate::execution::ExecutorError;
use crate::storage::{Storage, Table};
use crate::types::TableId;
use crate::types::tuple::Tuple;

pub struct Update { }

impl Update {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn execute(table_id: TableId, input: BoxedExecutor, values: BoxedExecutor, storage: impl Storage) {
        if let Some(table_catalog) = storage.get_catalog().get_table(&table_id) {
            let mut value_map = HashMap::new();

            // only once
            #[for_await]
            for tuple in values {
                let Tuple { columns, values, .. } = tuple?;
                for i in 0..columns.len() {
                    value_map.insert(columns[i].id, values[i].clone());
                }
            }

            let table = storage.get_table(&table_catalog.id)?;

            #[for_await]
            for tuple in input {
                let mut tuple = tuple?;

                for (i, column) in tuple.columns.iter().enumerate() {
                    if let Some(value) = value_map.get(&column.id) {
                        tuple.values[i] = value.clone();
                    }
                }

                table.append(tuple)?;
            }
        } else {
            Err(CatalogError::NotFound("root", table_id.to_string()))?;
        }
    }
}