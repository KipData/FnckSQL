use crate::catalog::ColumnRef;
use crate::types::value::ValueRef;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct ValuesOperator {
    pub rows: Vec<Vec<ValueRef>>,
    pub columns: Vec<ColumnRef>,
}
