use crate::catalog::TableName;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct TruncateOperator {
    /// Table name to insert to
    pub table_name: TableName,
}
