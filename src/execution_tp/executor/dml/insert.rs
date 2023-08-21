use std::collections::HashMap;
use futures_async_stream::try_stream;
use crate::catalog::CatalogError;
use crate::execution_tp::executor::BoxedExecutor;
use crate::execution_tp::ExecutorError;
use crate::storage_tp::{Storage, Table};
use crate::types::{ColumnId, TableId};
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;

pub struct Insert { }

impl Insert {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn execute(table_id: TableId, input: BoxedExecutor, storage: impl Storage) {
        if let Some(table_catalog) = storage.get_catalog().get_table(&table_id) {
            let table = storage.get_table(&table_catalog.id)?;

            #[for_await]
            for tuple in input {
                let Tuple { columns, values, .. } = tuple?;
                let mut tuple_map: HashMap<ColumnId, DataValue> = values
                    .into_iter()
                    .enumerate()
                    .map(|(i, value)| (columns[i].id, value))
                    .collect();

                let all_columns = table_catalog.all_columns();

                let mut tuple = Tuple {
                    id: None,
                    columns: Vec::with_capacity(all_columns.len()),
                    values: Vec::with_capacity(all_columns.len()),
                };

                for (col_id, col) in all_columns {
                    let value = tuple_map.remove(col_id)
                        .unwrap_or_else(|| DataValue::none(col.datatype()));

                    tuple.columns.push(col.clone());
                    tuple.values.push(value)
                }

                table.append(tuple)?;
            }
        } else {
            Err(CatalogError::NotFound("root", table_id.to_string()))?;
        }
    }
}