use crate::binder::copy::ExtSource;
use crate::catalog::ColumnRef;
use crate::types::LogicalType;

#[derive(Debug, PartialEq, Clone)]
pub struct CopyFromFileOperator {
    pub table: String,
    pub source: ExtSource,
    pub types: Vec<LogicalType>,
    pub columns: Vec<ColumnRef>,
}