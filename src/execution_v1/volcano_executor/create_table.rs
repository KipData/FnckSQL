use crate::catalog::ColumnCatalog;
use crate::execution_v1::physical_plan::physical_create_table::PhysicalCreateTable;
use crate::execution_v1::ExecutorError;
use crate::storage::Storage;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;
use std::sync::Arc;

pub struct CreateTable {}

impl CreateTable {
    #[try_stream(boxed, ok = RecordBatch , error = ExecutorError)]
    pub async fn execute(plan: PhysicalCreateTable, storage: impl Storage) {
        let mut columns = Vec::new();
        plan.columns.iter().for_each(|c| {
            columns.push(ColumnCatalog::new(c.0.clone(), c.1, c.2.clone()));
        });
        let table_name = plan.table_name.clone();
        // columns->batch record
        let mut data = Vec::new();

        columns.iter().for_each(|c| {
            let batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![c.to_field()])));
            data.push(batch);
        });

        storage.create_table(plan.table_name.as_str(), data);
    }
}
