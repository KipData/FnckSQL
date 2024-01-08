use crate::catalog::TableName;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct DeleteOperator {
    pub table_name: TableName,
}
