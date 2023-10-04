use crate::catalog::TableName;

#[derive(Debug, PartialEq, Clone)]
pub struct DropTableOperator {
    /// Table name to insert to
    pub table_name: TableName,
}
