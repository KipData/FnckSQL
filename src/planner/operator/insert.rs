use crate::expression::ScalarExpression;
use crate::types::ColumnIdx;

#[derive(Debug, PartialEq, Clone)]
pub struct InsertOperator {
    pub table: String,
    pub(crate) col_idxs: Vec<ColumnIdx>,
    pub(crate) cols: Vec<Vec<ScalarExpression>>,
}