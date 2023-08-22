use crate::catalog::ColumnRef;

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTableOperator {
    /// Table name to insert to
    pub table_name: String,
    /// List of columns of the table
    pub columns: Vec<ColumnRef>,
}
