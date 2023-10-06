use crate::catalog::TableName;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::update::UpdateOperator;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use std::cell::RefCell;
use std::collections::HashMap;

pub struct Update {
    table_name: TableName,
}

impl From<UpdateOperator> for Update {
    fn from(UpdateOperator { table_name }: UpdateOperator) -> Update {
        Update { table_name }
    }
}

impl<T: Transaction> Executor<T> for Update {
    fn execute(self, inputs: Vec<BoxedExecutor>, transaction: &RefCell<T>) -> BoxedExecutor {
        unsafe { self._execute(transaction.as_ptr().as_mut().unwrap(), inputs) }
    }
}

impl Update {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<T: Transaction>(
        self,
        transaction: &mut T,
        mut inputs: Vec<BoxedExecutor>,
    ) {
        let Update { table_name } = self;

        if let Some(table_catalog) = transaction.table(&table_name).cloned() {
            let mut value_map = HashMap::new();

            // only once
            #[for_await]
            for tuple in inputs.remove(1) {
                let Tuple {
                    columns, values, ..
                } = tuple?;
                for i in 0..columns.len() {
                    value_map.insert(columns[i].id, values[i].clone());
                }
            }
            #[for_await]
            for tuple in inputs.remove(0) {
                let mut tuple: Tuple = tuple?;
                let mut is_overwrite = true;

                for (i, column) in tuple.columns.iter().enumerate() {
                    if let Some(value) = value_map.get(&column.id) {
                        if column.desc.is_primary {
                            let old_key = tuple.id.replace(value.clone()).unwrap();

                            transaction.delete(&table_name, old_key)?;
                            is_overwrite = false;
                        }
                        if column.desc.is_unique && value != &tuple.values[i] {
                            if let Some(index_meta) =
                                table_catalog.get_unique_index(&column.id.unwrap())
                            {
                                let mut index = Index {
                                    id: index_meta.id,
                                    column_values: vec![tuple.values[i].clone()],
                                };
                                transaction.del_index(&table_name, &index)?;

                                if !value.is_null() {
                                    index.column_values[0] = value.clone();
                                    transaction.add_index(
                                        &table_name,
                                        index,
                                        vec![tuple.id.clone().unwrap()],
                                        true,
                                    )?;
                                }
                            }
                        }

                        tuple.values[i] = value.clone();
                    }
                }

                transaction.append(&table_name, tuple, is_overwrite)?;
            }
        }
    }
}
