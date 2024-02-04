use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::volcano::{build_read, BoxedExecutor, WriteExecutor};
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use itertools::Itertools;

pub struct Delete {
    table_name: TableName,
    input: LogicalPlan,
}

impl From<(DeleteOperator, LogicalPlan)> for Delete {
    fn from((DeleteOperator { table_name, .. }, input): (DeleteOperator, LogicalPlan)) -> Self {
        Delete { table_name, input }
    }
}

impl<T: Transaction> WriteExecutor<T> for Delete {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl Delete {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    async fn _execute<T: Transaction>(self, transaction: &mut T) {
        let Delete { table_name, input } = self;
        let option_index_metas = transaction.table(table_name.clone()).map(|table_catalog| {
            table_catalog
                .columns_with_id()
                .enumerate()
                .filter_map(|(i, (_, col))| {
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
            let mut tuple_ids = Vec::new();
            let mut indexes = Vec::new();

            #[for_await]
            for tuple in build_read(input, transaction) {
                let tuple: Tuple = tuple?;

                for (i, index_meta) in index_metas.iter() {
                    let value = &tuple.values[*i];

                    if !value.is_null() {
                        let index = Index {
                            id: index_meta.id,
                            column_values: vec![value.clone()],
                        };

                        indexes.push(index);
                    }
                }

                if let Some(tuple_id) = tuple.id {
                    tuple_ids.push(tuple_id);
                }
            }
            for index in indexes {
                transaction.del_index(&table_name, &index)?;
            }
            for tuple_id in tuple_ids {
                transaction.delete(&table_name, tuple_id)?;
            }
        }
    }
}
