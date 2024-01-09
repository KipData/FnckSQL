use crate::expression::ScalarExpression;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct ProjectOperator {
    pub exprs: Vec<ScalarExpression>,
}
