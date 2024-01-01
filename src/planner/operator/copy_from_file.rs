use crate::binder::copy::ExtSource;
use crate::catalog::ColumnRef;

#[derive(Debug, PartialEq, Clone)]
pub struct CopyFromFileOperator {
    pub table: String,
    pub source: ExtSource,
    pub columns: Vec<ColumnRef>,
}
