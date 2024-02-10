use crate::catalog::{ColumnCatalog, TableMeta};
use crate::errors::DatabaseError;
use crate::execution::volcano::{BoxedExecutor, ReadExecutor};
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use futures_async_stream::try_stream;
use std::sync::Arc;

pub struct ShowTables;

impl<T: Transaction> ReadExecutor<T> for ShowTables {
    fn execute(self, transaction: &T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl ShowTables {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
        let metas = transaction.table_metas()?;

        for TableMeta {
            table_name,
            colum_meta_paths: histogram_paths,
        } in metas
        {
            let schema_ref = Arc::new(vec![
                Arc::new(ColumnCatalog::new_dummy("TABLE".to_string())),
                Arc::new(ColumnCatalog::new_dummy("COLUMN_METAS_LEN".to_string())),
            ]);
            let values = vec![
                Arc::new(DataValue::Utf8(Some(table_name.to_string()))),
                Arc::new(DataValue::UInt32(Some(histogram_paths.len() as u32))),
            ];

            yield Tuple {
                id: None,
                schema_ref,
                values,
            };
        }
    }
}
