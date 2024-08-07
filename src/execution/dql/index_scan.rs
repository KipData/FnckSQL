use crate::execution::{Executor, ReadExecutor};
use crate::expression::range_detacher::Range;
use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Iter, Transaction};
use crate::throw;
use crate::types::index::IndexMetaRef;

pub(crate) struct IndexScan {
    op: ScanOperator,
    index_by: IndexMetaRef,
    ranges: Vec<Range>,
}

impl From<(ScanOperator, IndexMetaRef, Range)> for IndexScan {
    fn from((op, index_by, range): (ScanOperator, IndexMetaRef, Range)) -> Self {
        let ranges = match range {
            Range::SortedRanges(ranges) => ranges,
            range => vec![range],
        };

        IndexScan {
            op,
            index_by,
            ranges,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for IndexScan {
    fn execute(self, transaction: &'a T) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let ScanOperator {
                    table_name,
                    columns,
                    limit,
                    ..
                } = self.op;

                let mut iter = transaction
                    .read_by_index(table_name, limit, columns, self.index_by, self.ranges)
                    .unwrap();

                while let Some(tuple) = throw!(iter.next_tuple()) {
                    yield Ok(tuple);
                }
            },
        )
    }
}
