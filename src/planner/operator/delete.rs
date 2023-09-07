use crate::catalog::TableName;

#[derive(Debug, PartialEq, Clone)]
pub struct DeleteOperator {
    pub table_name: TableName,
}