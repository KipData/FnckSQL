use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::function::scala::FuncMonotonicity;
use crate::expression::function::scala::ScalarFunctionImpl;
use crate::expression::function::FunctionSummary;
use crate::expression::ScalarExpression;
use crate::scala_function;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use chrono::{Datelike, Local};
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

scala_function!(CurrentDate::current_date() -> LogicalType::Date => (|| {
    Ok(DataValue::Date32(Some(Local::now().num_days_from_ce())))
}));
