use std::collections::HashMap;
use futures_async_stream::try_stream;
use crate::catalog::TableName;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::update::UpdateOperator;
use crate::storage::{Storage, Transaction};
use crate::types::index::Index;
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

        if let Some(mut transaction) = storage.transaction(&table_name).await {
            let table_catalog = storage.table(&table_name).await.unwrap();
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
                let mut tuple: Tuple = tuple?;
                let mut is_overwrite = true;

                for (i, column) in tuple.columns.iter().enumerate() {
                    if let Some(value) = value_map.get(&column.id) {
                        if column.desc.is_primary {
                            let old_key = tuple.id.replace(value.clone()).unwrap();

                            transaction.delete(old_key)?;
                            is_overwrite = false;
                        }
                        if column.desc.is_unique && value != &tuple.values[i] {
                            if let Some(index_meta) = table_catalog.get_unique_index(&column.id) {
                                let mut index = Index {
                                    id: index_meta.id,
                                    column_values: vec![tuple.values[i].clone()],
                                };
                                transaction.del_index(&index)?;

                                if !value.is_null() {
                                    index.column_values[0] = value.clone();
                                    transaction.add_index(index, vec![tuple.id.clone().unwrap()], true)?;
                                }
                            }
                        }

                        tuple.values[i] = value.clone();
                    }
                }

                transaction.append(tuple, is_overwrite)?;
            }

            transaction.commit().await?;
        }
    }
}