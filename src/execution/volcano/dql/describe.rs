use crate::catalog::{ColumnCatalog, TableName};
use crate::execution::volcano::DatabaseError;
use crate::execution::volcano::{BoxedExecutor, ReadExecutor};
use crate::planner::operator::describe::DescribeOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type, ValueRef};
use futures_async_stream::try_stream;
use lazy_static::lazy_static;
use std::sync::Arc;
use sqlparser::ast::CharLengthUnits;

lazy_static! {
    static ref PRIMARY_KEY_TYPE: ValueRef = Arc::new(DataValue::Utf8 {
        value: Some(String::from("PRIMARY")),
        ty: Utf8Type::Variable(None),
        unit: CharLengthUnits::Characters
    });
    static ref UNIQUE_KEY_TYPE: ValueRef = Arc::new(DataValue::Utf8 {
        value: Some(String::from("UNIQUE")),
        ty: Utf8Type::Variable(None),
        unit: CharLengthUnits::Characters
    });
    static ref EMPTY_KEY_TYPE: ValueRef = Arc::new(DataValue::Utf8 {
        value: Some(String::from("EMPTY")),
        ty: Utf8Type::Variable(None),
        unit: CharLengthUnits::Characters
    });
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
            let datatype = column.datatype();
            let default = column
                .desc
                .default
                .as_ref()
                .map(|expr| format!("{}", expr))
                .unwrap_or_else(|| "null".to_string());
            let values = vec![
                Arc::new(DataValue::Utf8 {
                    value: Some(column.name().to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }),
                Arc::new(DataValue::Utf8 {
                    value: Some(datatype.to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }),
                Arc::new(DataValue::Utf8 {
                    value: datatype.raw_len().map(|len| len.to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }),
                Arc::new(DataValue::Utf8 {
                    value: Some(column.nullable.to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }),
                key_fn(column),
                Arc::new(DataValue::Utf8 {
                    value: Some(default),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }),
            ];
            yield Tuple { id: None, values };
        }
    }
}
