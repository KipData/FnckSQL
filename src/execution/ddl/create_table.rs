use crate::execution::{Executor, WriteExecutor};
use crate::planner::operator::create_table::CreateTableOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple_builder::TupleBuilder;

pub struct CreateTable {
    op: CreateTableOperator,
}

impl From<CreateTableOperator> for CreateTable {
    fn from(op: CreateTableOperator) -> Self {
        CreateTable { op }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CreateTable {
    fn execute_mut(
        self,
        (table_cache, _, _): (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let CreateTableOperator {
                    table_name,
                    columns,
                    if_not_exists,
                } = self.op;

                let _ = throw!(unsafe { &mut (*transaction) }.create_table(
                    table_cache,
                    table_name.clone(),
                    columns,
                    if_not_exists
                ));

                yield Ok(TupleBuilder::build_result(format!("{}", table_name)));
            },
        )
    }
}
