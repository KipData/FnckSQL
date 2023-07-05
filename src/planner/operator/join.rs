use std::sync::Arc;

use crate::{expression::ScalarExpression, planner::logical_select_plan::LogicalSelectPlan};

use super::Operator;

#[derive(Debug, PartialEq, Clone)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinOperator {
    pub on: Option<ScalarExpression>,
    pub join_type: JoinType,
}

impl JoinOperator {
    pub fn new(
        left: LogicalSelectPlan,
        right: LogicalSelectPlan,
        on: Option<ScalarExpression>,
        join_type: JoinType,
    ) -> LogicalSelectPlan {
        LogicalSelectPlan {
            operator: Arc::new(Operator::Join(JoinOperator { on, join_type })),
            children: vec![Arc::new(left), Arc::new(right)],
        }
    }
}
