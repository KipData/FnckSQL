use crate::catalog::TableName;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct DropTableOperator {
    /// Table name to insert to
    pub table_name: TableName,
    pub if_exists: bool,
}
