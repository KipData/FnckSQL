use std::{sync::Arc, vec};

use crate::expression::ScalarExpression;
use crate::planner::LogicalPlan;

use super::Operator;

#[derive(Debug, PartialEq, Clone)]
pub struct FilterOperator {
    pub predicate: ScalarExpression,
    having: bool,
}

impl FilterOperator {
    pub fn new(
        predicate: ScalarExpression,
        children: LogicalPlan,
        having: bool,
    ) -> LogicalPlan {
        LogicalPlan {
            operator: Arc::new(Operator::Filter(FilterOperator { predicate, having })),
            children: vec![Arc::new(children)],
        }
    }
}
