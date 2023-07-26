use crate::execution_v1::physical_plan::physical_create_table::PhysicalCreateTable;
use crate::execution_v1::ExecutorError;
use crate::storage::Storage;
use crate::catalog::ColumnCatalog;
use std::sync::Arc;
use itertools::Itertools;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;

pub struct CreateTable {}

impl CreateTable {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(plan: PhysicalCreateTable, storage: impl Storage) {
        let fields = plan.columns
            .iter()
            .map(ColumnCatalog::to_field)
            .collect_vec();
        let schema = Arc::new(Schema::new(fields));

        let _ = storage.create_table(
            plan.table_name.as_str(),
            vec![RecordBatch::new_empty(schema)]
        );
    }
}
