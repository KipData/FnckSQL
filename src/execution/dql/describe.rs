use crate::catalog::{ColumnCatalog, TableName};
use crate::execution::DatabaseError;
use crate::execution::{Executor, ReadExecutor};
use crate::planner::operator::describe::DescribeOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use sqlparser::ast::CharLengthUnits;
use std::sync::LazyLock;

static PRIMARY_KEY_TYPE: LazyLock<DataValue> = LazyLock::new(|| DataValue::Utf8 {
    value: String::from("PRIMARY"),
    ty: Utf8Type::Variable(None),
    unit: CharLengthUnits::Characters,
});

static UNIQUE_KEY_TYPE: LazyLock<DataValue> = LazyLock::new(|| DataValue::Utf8 {
    value: String::from("UNIQUE"),
    ty: Utf8Type::Variable(None),
    unit: CharLengthUnits::Characters,
});

static EMPTY_KEY_TYPE: LazyLock<DataValue> = LazyLock::new(|| DataValue::Utf8 {
    value: String::from("EMPTY"),
    ty: Utf8Type::Variable(None),
    unit: CharLengthUnits::Characters,
});

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
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let table = throw!(throw!(
                    unsafe { &mut (*transaction) }.table(cache.0, self.table_name.clone())
                )
                .ok_or(DatabaseError::TableNotFound));
                let key_fn = |column: &ColumnCatalog| {
                    if column.desc().is_primary() {
                        PRIMARY_KEY_TYPE.clone()
                    } else if column.desc().is_unique() {
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
                        DataValue::Utf8 {
                            value: column.name().to_string(),
                            ty: Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        },
                        DataValue::Utf8 {
                            value: datatype.to_string(),
                            ty: Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        },
                        DataValue::Utf8 {
                            value: datatype
                                .raw_len()
                                .map(|len| len.to_string())
                                .unwrap_or_else(|| "variable".to_string()),
                            ty: Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        },
                        DataValue::Utf8 {
                            value: column.nullable().to_string(),
                            ty: Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        },
                        key_fn(column),
                        DataValue::Utf8 {
                            value: default,
                            ty: Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        },
                    ];
                    yield Ok(Tuple::new(None, values));
                }
            },
        )
    }
}
