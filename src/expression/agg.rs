use std::fmt;
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

impl fmt::Display for AggKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AggKind::Count => write!(f, "Count"),
            AggKind::Sum => write!(f, "Sum"),
            AggKind::Min => write!(f, "Min"),
            AggKind::Max => write!(f, "Max"),
            AggKind::Avg => write!(f, "MinMax"),
            AggKind::RowCount => write!(f, "RowCount"),

        }
    }
}
