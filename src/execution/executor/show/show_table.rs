use crate::catalog::ColumnCatalog;
use crate::catalog::ColumnRef;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::show::ShowTablesOperator;
use crate::storage::Storage;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, ValueRef};
use futures_async_stream::try_stream;
use std::sync::Arc;

pub struct ShowTables {
    _op: ShowTablesOperator,
}

impl From<ShowTablesOperator> for ShowTables {
    fn from(op: ShowTablesOperator) -> Self {
        ShowTables { _op: op }
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
        let tables = storage.show_tables().await?;

        for table in tables {
            let columns: Vec<ColumnRef> =
                vec![Arc::new(ColumnCatalog::new_dummy("TABLES".to_string()))];
            let values: Vec<ValueRef> = vec![Arc::new(DataValue::Utf8(Some(table)))];

            yield Tuple {
                id: None,
                columns,
                values,
            };
        }
    }
}
