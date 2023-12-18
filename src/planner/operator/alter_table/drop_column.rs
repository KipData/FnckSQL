use crate::catalog::TableName;

#[derive(Debug, PartialEq, Clone)]
pub struct DropColumnOperator {
    pub table_name: TableName,
    pub column_name: String,
    pub if_exists: bool,
}
