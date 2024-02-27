use crate::errors::DatabaseError;
use crate::execution::volcano::{BoxedExecutor, WriteExecutor};
use crate::planner::operator::drop_table::DropTableOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use futures_async_stream::try_stream;

pub struct DropTable {
    op: DropTableOperator,
}

impl From<DropTableOperator> for DropTable {
    fn from(op: DropTableOperator) -> Self {
        DropTable { op }
    }
}

impl<T: Transaction> WriteExecutor<T> for DropTable {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl DropTable {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let DropTableOperator {
            table_name,
            if_exists,
        } = self.op;
        transaction.drop_table(&table_name, if_exists)?;

        yield TupleBuilder::build_result(format!("{}", table_name));
    }
}
