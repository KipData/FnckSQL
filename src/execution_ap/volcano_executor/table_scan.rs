
use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;
use crate::execution_ap::ExecutorError;
use crate::execution_ap::physical_plan::physical_table_scan::PhysicalTableScan;
use crate::planner::operator::scan::ScanOperator;
use crate::storage_ap::{Storage, Table, Transaction};

pub struct TableScan { }

impl TableScan {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(plan: PhysicalTableScan, storage: impl Storage) {
        // TODO: sort_fields, pre_where, limit
        let ScanOperator { table_id,  columns, limit, .. } = plan.op;

        let table = storage.get_table(&table_id)?;

        let mut transaction = table.read(
            limit,
            columns
        )?;

        while let Some(batch) =  transaction.next_batch()? {
            yield batch;
        }
    }
}