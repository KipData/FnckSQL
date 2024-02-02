use crate::errors::DatabaseError;
use crate::execution::volcano::{BoxedExecutor, WriteExecutor};
use crate::planner::operator::create_table::CreateTableOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use futures_async_stream::try_stream;

pub struct CreateTable {
    op: CreateTableOperator,
}

impl From<CreateTableOperator> for CreateTable {
    fn from(op: CreateTableOperator) -> Self {
        CreateTable { op }
    }
}

impl<T: Transaction> WriteExecutor<T> for CreateTable {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl CreateTable {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let CreateTableOperator {
            table_name,
            columns,
            if_not_exists,
        } = self.op;
        let _ = transaction.create_table(table_name.clone(), columns, if_not_exists)?;

        yield TupleBuilder::build_result(
            "CREATE TABLE SUCCESS".to_string(),
            format!("{}", table_name),
        )?;
    }
}
