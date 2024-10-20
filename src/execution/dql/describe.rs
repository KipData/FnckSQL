use crate::catalog::{ColumnCatalog, TableName};
use crate::execution::DatabaseError;
use crate::execution::{Executor, ReadExecutor};
use crate::planner::operator::describe::DescribeOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type, ValueRef};
use lazy_static::lazy_static;
use sqlparser::ast::CharLengthUnits;
use std::sync::Arc;

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

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Describe {
    fn execute(
        self,
        cache: (&'a TableCache, &'a StatisticsMetaCache),
        transaction: &'a T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let table = throw!(transaction
                    .table(cache.0, self.table_name.clone())
                    .ok_or(DatabaseError::TableNotFound));
                let key_fn = |column: &ColumnCatalog| {
                    if column.desc().is_primary {
                        PRIMARY_KEY_TYPE.clone()
                    } else if column.desc().is_unique {
                        UNIQUE_KEY_TYPE.clone()
                    } else {
                        EMPTY_KEY_TYPE.clone()
                    }
                };

                for column in table.columns() {
                    let datatype = column.datatype();
                    let default = column
                        .desc()
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
                            value: Some(column.nullable().to_string()),
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
                    yield Ok(Tuple { id: None, values });
                }
            },
        )
    }
}
