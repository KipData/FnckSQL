use std::sync::Arc;

use crate::expression::ScalarExpression;
use crate::planner::LogicalPlan;

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
        left: LogicalPlan,
        right: LogicalPlan,
        on: Option<ScalarExpression>,
        join_type: JoinType,
    ) -> LogicalPlan {
        LogicalPlan {
            operator: Arc::new(Operator::Join(JoinOperator { on, join_type })),
            children: vec![Arc::new(left), Arc::new(right)],
        }
    }
}
