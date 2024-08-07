use crate::execution::{Executor, WriteExecutor};
use crate::planner::operator::create_table::CreateTableOperator;
use crate::storage::Transaction;
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
    fn execute_mut(self, transaction: &'a mut T) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let CreateTableOperator {
                    table_name,
                    columns,
                    if_not_exists,
                } = self.op;

                let _ =
                    throw!(transaction.create_table(table_name.clone(), columns, if_not_exists));

                yield Ok(TupleBuilder::build_result(format!("{}", table_name)));
            },
        )
    }
}
