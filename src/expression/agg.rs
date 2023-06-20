use crate::types::DataType;

use super::ScalarExpression;

#[derive(Debug, Clone, PartialEq)]
pub enum AggKind {
    Avg,
    RowCount,
    Max,
    Min,
    Sum,
    Count,
}

pub struct AggCall {
    pub kind: AggKind,
    pub args: Vec<ScalarExpression>,
    pub return_type: DataType,
    // TODO: add distinct keyword
}
