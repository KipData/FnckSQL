use super::ScalarExpression;
use crate::types::LogicalType;

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
    pub return_type: LogicalType,
    // TODO: add distinct keyword
}
