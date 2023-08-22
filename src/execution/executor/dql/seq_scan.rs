use futures_async_stream::try_stream;
use crate::execution::ExecutorError;
use crate::execution::physical_plan::physical_table_scan::PhysicalTableScan;
use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Storage, Table, Transaction};
use crate::types::tuple::Tuple;

pub(crate) struct SeqScan { }

impl SeqScan {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn execute(plan: PhysicalTableScan, storage: impl Storage) {
        // TODO: sort_fields, pre_where, limit
        let ScanOperator { table_id,  columns, limit, .. } = plan.op;

        let table = storage.get_table(&table_id)?;

        let mut transaction = table.read(
            limit,
            columns
        )?;

        while let Some(tuple) =  transaction.next_tuple()? {
            yield tuple;
        }
    }
}