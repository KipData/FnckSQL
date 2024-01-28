use crate::catalog::{ColumnRef, TableName};

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct DeleteOperator {
    pub table_name: TableName,
    // for column pruning
    pub primary_key_column: ColumnRef,
}
