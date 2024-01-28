use crate::catalog::ColumnRef;
use crate::catalog::{ColumnCatalog, TableMeta};
use crate::execution::volcano::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::planner::operator::show::ShowTablesOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, ValueRef};
use futures_async_stream::try_stream;
use std::cell::RefCell;
use std::sync::Arc;

pub struct ShowTables {
    _op: ShowTablesOperator,
}

impl From<ShowTablesOperator> for ShowTables {
    fn from(op: ShowTablesOperator) -> Self {
        ShowTables { _op: op }
    }
}

impl<T: Transaction> Executor<T> for ShowTables {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor {
        unsafe { self._execute(transaction.as_ptr().as_ref().unwrap()) }
    }
}

impl ShowTables {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
        let metas = transaction.table_metas()?;

        for TableMeta {
            table_name,
            colum_meta_paths: histogram_paths,
        } in metas
        {
            let columns: Vec<ColumnRef> = vec![
                Arc::new(ColumnCatalog::new_dummy("TABLE".to_string())),
                Arc::new(ColumnCatalog::new_dummy("COLUMN_METAS_LEN".to_string())),
            ];
            let values: Vec<ValueRef> = vec![
                Arc::new(DataValue::Utf8(Some(table_name.to_string()))),
                Arc::new(DataValue::UInt32(Some(histogram_paths.len() as u32))),
            ];

            yield Tuple {
                id: None,
                columns,
                values,
            };
        }
    }
}
