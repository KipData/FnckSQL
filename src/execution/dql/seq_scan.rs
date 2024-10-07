use crate::execution::{Executor, ReadExecutor};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::storage::{Iter, StatisticsMetaCache, TableCache, Transaction};
use crate::throw;

pub(crate) struct SeqScan {
    op: TableScanOperator,
}

impl From<TableScanOperator> for SeqScan {
    fn from(op: TableScanOperator) -> Self {
        SeqScan { op }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for SeqScan {
    fn execute(
        self,
        (table_cache, _): (&'a TableCache, &'a StatisticsMetaCache),
        transaction: &'a T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let TableScanOperator {
                    table_name,
                    columns,
                    limit,
                    ..
                } = self.op;

                let mut iter = throw!(transaction.read(table_cache, table_name, limit, columns));

                while let Some(tuple) = throw!(iter.next_tuple()) {
                    yield Ok(tuple);
                }
            },
        )
    }
}
