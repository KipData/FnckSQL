use crate::catalog::TableName;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::delete::DeleteOperator;
use crate::storage::{Storage, Transaction};
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use itertools::Itertools;

pub struct Delete {
    table_name: TableName,
    input: BoxedExecutor,
}

impl From<(DeleteOperator, BoxedExecutor)> for Delete {
    fn from((DeleteOperator { table_name }, input): (DeleteOperator, BoxedExecutor)) -> Self {
        Delete { table_name, input }
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

        if let Some(mut transaction) = storage.transaction(&table_name).await {
            let table_catalog = storage.table(&table_name).await.unwrap();

            let vec = table_catalog
                .all_columns()
                .into_iter()
                .enumerate()
                .filter_map(|(i, col)| {
                    col.desc
                        .is_unique
                        .then(|| {
                            col.id.and_then(|col_id| {
                                table_catalog
                                    .get_unique_index(&col_id)
                                    .map(|index_meta| (i, index_meta))
                            })
                        })
                        .flatten()
                })
                .collect_vec();

            #[for_await]
            for tuple in input {
                let tuple: Tuple = tuple?;

                for (i, index_meta) in vec.iter() {
                    let value = &tuple.values[*i];

                    if !value.is_null() {
                        let index = Index {
                            id: index_meta.id,
                            column_values: vec![value.clone()],
                        };

                        transaction.del_index(&index)?;
                    }
                }

                if let Some(tuple_id) = tuple.id {
                    transaction.delete(tuple_id)?;
                }
            }
            transaction.commit().await?;
        }
    }
}
