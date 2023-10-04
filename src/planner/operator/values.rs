use crate::catalog::ColumnRef;
use crate::types::value::ValueRef;

#[derive(Debug, PartialEq, Clone)]
pub struct ValuesOperator {
    pub rows: Vec<Vec<ValueRef>>,
    pub columns: Vec<ColumnRef>,
}
