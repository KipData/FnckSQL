use crate::expression::ScalarExpression;
use crate::types::ColumnIdx;

#[derive(Debug)]
pub struct PhysicalInsert {
    /// Table name to insert to
    pub table_name: String,

    pub col_idxs: Vec<ColumnIdx>,
    /// List of columns of the table
    pub cols: Vec<Vec<ScalarExpression>>
}