use crate::expression::ScalarExpression;

#[derive(Debug, PartialEq, Clone)]
pub struct ProjectOperator {
    pub columns: Vec<ScalarExpression>,
}
