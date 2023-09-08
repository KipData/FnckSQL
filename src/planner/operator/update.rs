use crate::catalog::TableName;

#[derive(Debug, PartialEq, Clone)]
pub struct UpdateOperator {
    pub table_name: TableName,
}