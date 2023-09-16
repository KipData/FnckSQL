use crate::catalog::TableName;

#[derive(Debug, PartialEq, Clone)]
pub struct InsertOperator {
    pub table_name: TableName,
    pub is_overwrite: bool,
}