use std::collections::HashMap;
use futures_async_stream::try_stream;
use crate::catalog::TableName;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::update::UpdateOperator;
use crate::storage::{Storage, Table};
use crate::types::tuple::Tuple;

pub struct Update {
    table_name: TableName,
    input: BoxedExecutor,
    values: BoxedExecutor
}

impl From<(UpdateOperator, BoxedExecutor, BoxedExecutor)> for Update {
    fn from((UpdateOperator { table_name }, input, values): (UpdateOperator, BoxedExecutor, BoxedExecutor)) -> Self {
        Update {
            table_name,
            input,
            values
        }
    }
}

impl<S: Storage> Executor<S> for Update {
    fn execute(self, storage: &S) -> BoxedExecutor {
        self._execute(storage.clone())
    }
}

impl Update {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<S: Storage>(self, storage: S) {
        let Update { table_name, input, values } = self;

        if let Some(mut table) = storage.table(&table_name).await {
            let mut value_map = HashMap::new();

            // only once
            #[for_await]
            for tuple in values {
                let Tuple { columns, values, .. } = tuple?;
                for i in 0..columns.len() {
                    value_map.insert(columns[i].id, values[i].clone());
                }
            }
            #[for_await]
            for tuple in input {
                let mut tuple = tuple?;
                let mut is_overwrite = true;

                for (i, column) in tuple.columns.iter().enumerate() {
                    if let Some(value) = value_map.get(&column.id) {
                        if column.desc.is_primary {
                            if let Some(old_key) = tuple.id.replace(value.clone()) {
                                table.delete(old_key)?;
                                is_overwrite = false;
                            }
                        }
                        tuple.values[i] = value.clone();
                    }
                }

                table.append(tuple, is_overwrite)?;
            }
            table.commit().await?;
        }
    }
}