use crate::expression::ScalarExpression;
use crate::planner::LogicalPlan;

use super::Operator;

#[derive(Debug, PartialEq, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}
#[derive(Debug, Clone, PartialEq)]
pub enum JoinCondition {
    On {
        /// Equijoin clause expressed as pairs of (left, right) join columns
        on: Vec<(ScalarExpression, ScalarExpression)>,
        /// Filters applied during join (non-equi conditions)
        filter: Option<ScalarExpression>,
    },
    None,
}

#[derive(Debug, PartialEq, Clone)]
pub struct JoinOperator {
    pub on: JoinCondition,
    pub join_type: JoinType,
}

impl JoinOperator {
    pub fn new(
        left: LogicalPlan,
        right: LogicalPlan,
        on: JoinCondition,
        join_type: JoinType,
    ) -> LogicalPlan {
        LogicalPlan {
            operator: Operator::Join(JoinOperator { on, join_type }),
            childrens: vec![left, right],
        }
    }
}
