use crate::catalog::ColumnDesc;
use crate::execution_v1::physical_plan::PhysicalOperator;
use crate::expression::ScalarExpression;

pub struct PhysicalCreateTable {
    /// Table name to insert to
    pub table_name: String,
    /// List of columns of the table
    pub columns: Vec<(String, bool, ColumnDesc)>,
}
