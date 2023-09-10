use futures_async_stream::try_stream;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::drop_table::DropTableOperator;
use crate::storage::Storage;
use crate::types::tuple::Tuple;

pub struct DropTable {
    op: DropTableOperator
}

impl From<DropTableOperator> for DropTable {
    fn from(op: DropTableOperator) -> Self {
        DropTable {
            op
        }
    }
}

impl<S: Storage> Executor<S> for DropTable {
    fn execute(self, storage: &S) -> BoxedExecutor {
        self._execute(storage.clone())
    }
}

impl DropTable {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<S: Storage>(self, storage: S) {
        let DropTableOperator { table_name } = self.op;

        storage.drop_table(&table_name).await?;
    }
}