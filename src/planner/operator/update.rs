use crate::catalog::TableName;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct UpdateOperator {
    pub table_name: TableName,
}
