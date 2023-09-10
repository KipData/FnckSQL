use futures_async_stream::try_stream;
use crate::catalog::TableName;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::delete::DeleteOperator;
use crate::storage::{Storage, Table};
use crate::types::tuple::Tuple;

pub struct Delete {
    table_name: TableName,
    input: BoxedExecutor,
}

impl From<(DeleteOperator, BoxedExecutor)> for Delete {
    fn from((DeleteOperator { table_name }, input): (DeleteOperator, BoxedExecutor)) -> Self {
        Delete {
            table_name,
            input,
        }
    }
}

impl<S: Storage> Executor<S> for Delete {
    fn execute(self, storage: &S) -> BoxedExecutor {
        self._execute(storage.clone())
    }
}

impl Delete {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<S: Storage>(self, storage: S) {
        let Delete { table_name, input } = self;

        if let Some(mut table) = storage.table(&table_name).await {
            #[for_await]
            for tuple in input {
                let tuple: Tuple = tuple?;

                if let Some(tuple_id) = tuple.id {
                    table.delete(tuple_id)?;
                }
            }
            table.commit().await?;
        }
    }
}