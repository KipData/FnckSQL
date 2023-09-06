use std::collections::HashMap;
use futures_async_stream::try_stream;
use crate::catalog::TableName;
use crate::execution::executor::BoxedExecutor;
use crate::execution::ExecutorError;
use crate::storage::{Storage, Table};
use crate::types::tuple::Tuple;

pub struct Update { }

impl Update {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn execute(table_name: TableName, input: BoxedExecutor, values: BoxedExecutor, storage: impl Storage) {
        if let Some(mut table) = storage.table(&table_name).await {
            let mut value_map = HashMap::new();

            // only once
            #[for_await]
            for tuple in values {
                let Tuple { columns, values, .. } = tuple?;
                for i in 0..columns.len() {
                    value_map.insert(columns[i].id, values[i].clone());
                }
            }

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
        }
    }
}