use crate::planner::operator::insert::InsertOperator;

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalInsertPlan {
    pub operator: InsertOperator
}