use crate::catalog::TableName;

#[derive(Debug, PartialEq, Clone)]
pub struct TruncateOperator {
    /// Table name to insert to
    pub table_name: TableName,
}
