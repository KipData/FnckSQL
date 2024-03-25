use chrono::{Datelike, Local};
use crate::function;
use crate::types::LogicalType;
use crate::types::value::DataValue;
use serde::Serialize;
use serde::Deserialize;
use crate::expression::function::FunctionSummary;
use std::sync::Arc;
use crate::expression::function::ScalarFunctionImpl;
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;
use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::function::FuncMonotonicity;

function!(CurrentDate::current_date() -> LogicalType::Date => (|| {
    Ok(DataValue::Date32(Some(Local::now().num_days_from_ce())))
}));