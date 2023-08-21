use futures_async_stream::try_stream;
use crate::execution_ap::physical_plan::physical_create_table::PhysicalCreateTable;
use crate::execution_tp::ExecutorError;
use crate::storage_tp::Storage;
use crate::types::tuple::Tuple;

pub struct CreateTable {}

impl CreateTable {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn execute(plan: PhysicalCreateTable, storage: impl Storage) {
        let _ = storage.create_table(
            plan.op.table_name,
            plan.op.columns
        )?;
    }
}