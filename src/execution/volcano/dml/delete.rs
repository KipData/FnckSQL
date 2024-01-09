use crate::catalog::TableName;
use crate::execution::volcano::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::delete::DeleteOperator;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use itertools::Itertools;
use std::cell::RefCell;

pub struct Delete {
    table_name: TableName,
    input: BoxedExecutor,
}

impl From<(DeleteOperator, BoxedExecutor)> for Delete {
    fn from((DeleteOperator { table_name }, input): (DeleteOperator, BoxedExecutor)) -> Self {
        Delete { table_name, input }
    }
}

impl<T: Transaction> Executor<T> for Delete {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor {
        unsafe { self._execute(transaction.as_ptr().as_mut().unwrap()) }
    }
}

impl Delete {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let Delete { table_name, input } = self;
        let option_index_metas = transaction.table(table_name.clone()).map(|table_catalog| {
            table_catalog
                .all_columns()
                .into_iter()
                .enumerate()
                .filter_map(|(i, col)| {
                    col.desc
                        .is_unique
                        .then(|| {
                            col.id().and_then(|col_id| {
                                table_catalog
                                    .get_unique_index(&col_id)
                                    .map(|index_meta| (i, index_meta.clone()))
                            })
                        })
                        .flatten()
                })
                .collect_vec()
        });

        if let Some(index_metas) = option_index_metas {
            #[for_await]
            for tuple in input {
                let tuple: Tuple = tuple?;

                for (i, index_meta) in index_metas.iter() {
                    let value = &tuple.values[*i];

                    if !value.is_null() {
                        let index = Index {
                            id: index_meta.id,
                            column_values: vec![value.clone()],
                        };

                        transaction.del_index(&table_name, &index)?;
                    }
                }

                if let Some(tuple_id) = tuple.id {
                    transaction.delete(&table_name, tuple_id)?;
                }
            }
        }
    }
}
