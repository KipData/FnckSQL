use arrow::array::{Array, new_null_array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;
use itertools::Itertools;
use crate::catalog::CatalogError;
use crate::execution_v1::ExecutorError;
use crate::execution_v1::volcano_executor::BoxedExecutor;
use crate::storage::{Storage, Table};

pub struct Insert { }

impl Insert {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(table_name: String, input: BoxedExecutor, storage: impl Storage) {
        if let Some(table) = storage.get_catalog().get_table_by_name(&table_name) {
            #[for_await]
            for batch in input {
                let batch: RecordBatch = batch?;

                let projection_schema =  batch.schema();
                let fields = projection_schema.fields();
                let mut arrays = batch.columns().to_vec();
                let col_len = arrays[0].len();

                arrays.reverse();

                let full_arrays = table.all_columns()
                    .into_iter()
                    .map(|(_, col_catalog)| {
                        if fields.contains(&col_catalog.to_field()) {
                            arrays.pop().unwrap()
                        } else {
                            new_null_array(&DataType::from(col_catalog.datatype().clone()), col_len)
                        }
                    })
                    .collect_vec();

                let new_batch = RecordBatch::try_new(table.schema(), full_arrays)?;

                storage.get_table(table.id.unwrap())?.append(new_batch)?;
            }
        } else {
            Err(CatalogError::NotFound("root", table_name.to_string()))?;
        }
    }
}