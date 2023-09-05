use crate::catalog::{ColumnRef, TableName};

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTableOperator {
    /// Table name to insert to
    pub table_name: TableName,
    /// List of columns of the table
    pub columns: Vec<ColumnRef>,
}
