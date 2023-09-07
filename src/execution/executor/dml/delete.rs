use futures_async_stream::try_stream;
use crate::catalog::TableName;
use crate::execution::executor::BoxedExecutor;
use crate::execution::ExecutorError;
use crate::storage::{Storage, Table};
use crate::types::tuple::Tuple;

pub struct Delete { }

impl Delete {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn execute(table_name: TableName, input: BoxedExecutor, storage: impl Storage) {
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