use crate::catalog::TableName;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct InsertOperator {
    pub table_name: TableName,
    pub is_overwrite: bool,
}
