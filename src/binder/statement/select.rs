use crate::parser::Value;

/// A bound `SELECT` statement.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundSelect {
    pub values: Vec<Value>,
}
