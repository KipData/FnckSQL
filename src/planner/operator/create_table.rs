use crate::catalog::ColumnDesc;

#[derive(Debug, PartialEq, Clone)]
#[allow(dead_code)]
pub struct CreateTableOperator {
    /// Table name to insert to
    pub table_name: String,
    /// List of columns of the table
    pub columns: Vec<(String, bool, ColumnDesc)>,
}
