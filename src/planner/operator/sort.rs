use crate::expression::ScalarExpression;

#[derive(Debug, PartialEq, Clone)]
pub struct SortField {
    pub expr: ScalarExpression,
    pub desc: bool,
    pub nulls_first: bool,
}

impl SortField {
    pub fn new(expr: ScalarExpression, desc: bool, nulls_first: bool) -> Self {
        SortField {
            expr,
            desc,
            nulls_first,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SortOperator {
    pub sort_fields: Vec<SortField>,
    /// Support push down limit to sort plan.
    pub limit: Option<usize>,
}
