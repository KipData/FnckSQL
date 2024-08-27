use crate::catalog::ColumnCatalog;
use crate::catalog::ColumnDesc;
use crate::catalog::TableCatalog;
use crate::errors::DatabaseError;
use crate::expression::function::table::TableFunctionImpl;
use crate::expression::function::FunctionSummary;
use crate::expression::ScalarExpression;
use crate::table_function;
use crate::types::tuple::SchemaRef;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, ValueRef};
use crate::types::LogicalType;
use lazy_static::lazy_static;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

table_function!(Numbers::numbers(LogicalType::Integer) -> [number: LogicalType::Integer] => (|v1: ValueRef| {
    let num = v1.i32().ok_or_else(|| DatabaseError::NotNull)?;

    Ok(Box::new((0..num)
        .map(|i| Ok(Tuple {
            id: None,
            values: vec![
                Arc::new(DataValue::Int32(Some(i))),
            ]
        }))) as Box<dyn Iterator<Item = Result<Tuple, DatabaseError>>>)
}));
