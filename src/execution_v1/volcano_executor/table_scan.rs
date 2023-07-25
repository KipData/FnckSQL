
use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;
use crate::execution_v1::ExecutorError;
use crate::execution_v1::physical_plan::physical_table_scan::PhysicalTableScan;
use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Storage, Table, Transaction};

pub struct TableScan { }

impl TableScan {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(plan: PhysicalTableScan, storage: impl Storage) {
        // TODO: sort_fields, pre_where, limit
        let ScanOperator { table_ref_id,  .. } = plan.base;
        println!("ref id: {}", table_ref_id);
        let table = storage.get_table(table_ref_id)?;
        let mut transaction = table.read(
            None,
            None
        )?;

        while let Some(batch) =  transaction.next_batch()? {
            yield batch;
        }
    }
}