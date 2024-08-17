use crate::execution::{Executor, WriteExecutor};
use crate::planner::operator::truncate::TruncateOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction};
use crate::throw;
use crate::types::tuple_builder::TupleBuilder;

pub struct Truncate {
    op: TruncateOperator,
}

impl From<TruncateOperator> for Truncate {
    fn from(op: TruncateOperator) -> Self {
        Truncate { op }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Truncate {
    fn execute_mut(
        self,
        _: (&'a TableCache, &'a StatisticsMetaCache),
        transaction: &'a mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let TruncateOperator { table_name } = self.op;

                throw!(transaction.drop_data(&table_name));

                yield Ok(TupleBuilder::build_result(format!("{}", table_name)));
            },
        )
    }
}
