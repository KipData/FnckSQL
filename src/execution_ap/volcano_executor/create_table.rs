use crate::execution_ap::physical_plan::physical_create_table::PhysicalCreateTable;
use crate::execution_ap::ExecutorError;
use crate::storage_ap::Storage;
use crate::catalog::ColumnCatalog;
use std::sync::Arc;
use arrow::datatypes::Schema;
use itertools::Itertools;
use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;

pub struct CreateTable {}

impl CreateTable {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(plan: PhysicalCreateTable, storage: impl Storage) {
        let fields = plan.op.columns
            .iter()
            .map(ColumnCatalog::to_field)
            .collect_vec();
        let schema = Arc::new(Schema::new(fields));

        let _ = storage.create_table(
            plan.op.table_name.as_str(),
            vec![RecordBatch::new_empty(schema)]
        )?;
    }
}
