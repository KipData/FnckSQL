use crate::execution::{Executor, WriteExecutor};
use crate::planner::operator::drop_table::DropTableOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple_builder::TupleBuilder;

pub struct DropTable {
    op: DropTableOperator,
}

impl From<DropTableOperator> for DropTable {
    fn from(op: DropTableOperator) -> Self {
        DropTable { op }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for DropTable {
    fn execute_mut(
        self,
        (table_cache, _, _): (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: &'a mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let DropTableOperator {
                    table_name,
                    if_exists,
                } = self.op;

                throw!(transaction.drop_table(table_cache, table_name.clone(), if_exists));

                yield Ok(TupleBuilder::build_result(format!("{}", table_name)));
            },
        )
    }
}
