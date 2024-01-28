use crate::catalog::{ColumnRef, TableName};

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct AnalyzeOperator {
    pub table_name: TableName,
    pub columns: Vec<ColumnRef>,
}
