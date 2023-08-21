use ahash::HashMap;
use arrow::array::{Array, ArrayRef, new_null_array};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;
use itertools::Itertools;
use crate::catalog::CatalogError;
use crate::execution_ap::ExecutorError;
use crate::execution_ap::volcano_executor::BoxedExecutor;
use crate::storage_ap::{Storage, Table};
use crate::types::TableId;

pub struct Insert { }

impl Insert {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(table_id: TableId, input: BoxedExecutor, storage: impl Storage) {
        if let Some(table) = storage.get_catalog().get_table(&table_id) {
            #[for_await]
            for batch in input {
                let batch: RecordBatch = batch?;

                let projection_schema =  batch.schema();
                let fields = projection_schema.fields();
                let arrays = batch.columns();

                let col_len = arrays[0].len();
                let insert_values: HashMap<Field, ArrayRef> = (0..fields.len()).into_iter()
                    .map(|i| (fields[i].clone(), arrays[i].clone()))
                    .collect();

                let full_arrays = table.all_columns()
                    .into_iter()
                    .filter_map(|(_, col_catalog)| {
                        insert_values.get(&col_catalog.to_field())
                            .map(ArrayRef::clone)
                            .or_else(|| Some(new_null_array(&DataType::from(col_catalog.datatype().clone()), col_len)))
                    })
                    .collect_vec();

                let new_batch = RecordBatch::try_new(table.schema(), full_arrays)?;

                storage.get_table(&table.id)?.append(new_batch)?;
            }
        } else {
            Err(CatalogError::NotFound("root", table_id.to_string()))?;
        }
    }
}