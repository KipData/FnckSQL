use futures_async_stream::try_stream;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::show::ShowTablesOperator;
use crate::storage::Storage;
use crate::types::tuple::Tuple;
use crate::catalog::ColumnCatalog;
use crate::catalog::ColumnRef;
use std::sync::Arc;
use crate::types::value::{DataValue, ValueRef};

pub struct ShowTables {
    _op: ShowTablesOperator,
}

impl From<ShowTablesOperator> for ShowTables {
    fn from(op: ShowTablesOperator) -> Self {
        ShowTables {
            _op: op
        }
    }
}

impl<S: Storage> Executor<S> for ShowTables {
    fn execute(self, storage: &S) -> BoxedExecutor {
        self._execute(storage.clone())
    }
}

impl ShowTables {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<S: Storage>(self, storage: S) {
        if let Some(tables) = storage.show_tables().await {
            for (table,column_count) in tables {
                let columns: Vec<ColumnRef> = vec![
                    Arc::new(ColumnCatalog::new_dummy("TABLES".to_string())),
                    Arc::new(ColumnCatalog::new_dummy("COLUMN_COUNT".to_string())),
                ];
                let values: Vec<ValueRef> = vec![
                    Arc::new(DataValue::Utf8(Some(table))),
                    Arc::new(DataValue::UInt32(Some(column_count as u32))),
                ];

                yield Tuple {
                    id: None,
                    columns,
                    values,
                };
            }
        }
    }
}