use crate::catalog::TableMeta;
use crate::errors::DatabaseError;
use crate::execution::volcano::{BoxedExecutor, ReadExecutor};
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use futures_async_stream::try_stream;
use std::sync::Arc;
use sqlparser::ast::CharLengthUnits;

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

        for TableMeta { table_name } in metas {
            let values = vec![Arc::new(DataValue::Utf8 {
                value: Some(table_name.to_string()),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters
            })];

            yield Tuple { id: None, values };
        }
    }
}
