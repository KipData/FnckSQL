use crate::execution::{Executor, ReadExecutor};
use crate::expression::function::table::TableFunction;
use crate::planner::operator::function_scan::FunctionScanOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;

pub struct FunctionScan {
    table_function: TableFunction,
}

impl From<FunctionScanOperator> for FunctionScan {
    fn from(op: FunctionScanOperator) -> Self {
        FunctionScan {
            table_function: op.table_function,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for FunctionScan {
    fn execute(
        self,
        _: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        _: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let TableFunction { args, inner } = self.table_function;
                for tuple in throw!(inner.eval(&args)) {
                    yield tuple;
                }
            },
        )
    }
}
