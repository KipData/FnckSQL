use crate::catalog::{ColumnCatalog, TableName};
use crate::execution::volcano::DatabaseError;
use crate::execution::volcano::{BoxedExecutor, ReadExecutor};
use crate::planner::operator::describe::DescribeOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, ValueRef};
use futures_async_stream::try_stream;
use lazy_static::lazy_static;
use std::sync::Arc;

lazy_static! {
    static ref PRIMARY_KEY_TYPE: ValueRef =
        Arc::new(DataValue::Utf8(Some(String::from("PRIMARY"))));
    static ref UNIQUE_KEY_TYPE: ValueRef = Arc::new(DataValue::Utf8(Some(String::from("UNIQUE"))));
    static ref EMPTY_KEY_TYPE: ValueRef = Arc::new(DataValue::Utf8(Some(String::from("EMPTY"))));
}

pub struct Describe {
    table_name: TableName,
}

impl From<DescribeOperator> for Describe {
    fn from(op: DescribeOperator) -> Self {
        Describe {
            table_name: op.table_name,
        }
    }
}

impl<T: Transaction> ReadExecutor<T> for Describe {
    fn execute(self, transaction: &T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl Describe {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
        let table = transaction
            .table(self.table_name.clone())
            .ok_or(DatabaseError::TableNotFound)?;
        let key_fn = |column: &ColumnCatalog| {
            if column.desc.is_primary {
                PRIMARY_KEY_TYPE.clone()
            } else if column.desc.is_unique {
                UNIQUE_KEY_TYPE.clone()
            } else {
                EMPTY_KEY_TYPE.clone()
            }
        };

        for column in table.columns() {
            let values = vec![
                Arc::new(DataValue::Utf8(Some(column.name().to_string()))),
                Arc::new(DataValue::Utf8(Some(column.datatype().to_string()))),
                Arc::new(DataValue::Utf8(Some(column.nullable.to_string()))),
                key_fn(column),
                column
                    .default_value()
                    .unwrap_or_else(|| Arc::new(DataValue::none(column.datatype()))),
            ];
            yield Tuple { id: None, values };
        }
    }
}
