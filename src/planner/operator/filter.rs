use std::{sync::Arc, vec};

use crate::{expression::ScalarExpression, planner::logical_select_plan::LogicalSelectPlan};

use super::Operator;

#[derive(Debug)]
pub struct FilterOperator {
    pub predicate: ScalarExpression,
    having: bool,
}

impl FilterOperator {
    pub fn new(
        predicate: ScalarExpression,
        children: LogicalSelectPlan,
        having: bool,
    ) -> LogicalSelectPlan {
        LogicalSelectPlan {
            operator: Arc::new(Operator::Filter(FilterOperator { predicate, having })),
            children: vec![Arc::new(children)],
        }
    }
}
