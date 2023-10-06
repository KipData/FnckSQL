use crate::catalog::TableName;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::insert::InsertOperator;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use futures_async_stream::try_stream;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Insert {
    table_name: TableName,
    is_overwrite: bool,
}

impl From<InsertOperator> for Insert {
    fn from(
        InsertOperator {
            table_name,
            is_overwrite,
        }: InsertOperator,
    ) -> Insert {
        Insert {
            table_name,
            is_overwrite,
        }
    }
}

impl<T: Transaction> Executor<T> for Insert {
    fn execute(self, inputs: Vec<BoxedExecutor>, transaction: &RefCell<T>) -> BoxedExecutor {
        unsafe { self._execute(transaction.as_ptr().as_mut().unwrap(), inputs) }
    }
}

impl Insert {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<T: Transaction>(
        self,
        transaction: &mut T,
        mut inputs: Vec<BoxedExecutor>,
    ) {
        let Insert {
            table_name,
            is_overwrite,
        } = self;
        let mut primary_key_index = None;
        let mut unique_values = HashMap::new();

        if let Some(table_catalog) = transaction.table(&table_name).cloned() {
            #[for_await]
            for tuple in inputs.remove(0) {
                let Tuple {
                    columns, values, ..
                } = tuple?;
                let mut tuple_map = HashMap::new();
                for (i, value) in values.into_iter().enumerate() {
                    let col = &columns[i];

                    if let Some(col_id) = col.id {
                        tuple_map.insert(col_id, value);
                    }
                }
                let primary_col_id = primary_key_index.get_or_insert_with(|| {
                    columns
                        .iter()
                        .find(|col| col.desc.is_primary)
                        .map(|col| col.id.unwrap())
                        .unwrap()
                });
                let all_columns = table_catalog.all_columns_with_id();
                let tuple_id = tuple_map.get(primary_col_id).cloned().unwrap();
                let mut tuple = Tuple {
                    id: Some(tuple_id.clone()),
                    columns: Vec::with_capacity(all_columns.len()),
                    values: Vec::with_capacity(all_columns.len()),
                };
                for (col_id, col) in all_columns {
                    let value = tuple_map
                        .remove(col_id)
                        .unwrap_or_else(|| Arc::new(DataValue::none(col.datatype())));

                    if col.desc.is_unique && !value.is_null() {
                        unique_values
                            .entry(col.id)
                            .or_insert_with(|| vec![])
                            .push((tuple_id.clone(), value.clone()))
                    }
                    if value.is_null() && !col.nullable {
                        return Err(ExecutorError::InternalError(format!(
                            "Non-null fields do not allow null values to be passed in: {:?}",
                            col
                        )));
                    }

                    tuple.columns.push(col.clone());
                    tuple.values.push(value)
                }

                transaction.append(&table_name, tuple, is_overwrite)?;
            }
            // Unique Index
            for (col_id, values) in unique_values {
                if let Some(index_meta) = table_catalog.get_unique_index(&col_id.unwrap()) {
                    for (tuple_id, value) in values {
                        let index = Index {
                            id: index_meta.id,
                            column_values: vec![value],
                        };

                        transaction.add_index(&table_name, index, vec![tuple_id], true)?;
                    }
                }
            }
        }
    }
}
