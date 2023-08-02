use crate::execution_v1::physical_plan::PhysicalOperator;
use crate::planner::operator::limit::LimitOperator;

#[derive(Debug)]
pub struct PhysicalLimit {
    pub(crate) op: LimitOperator,
    pub(crate) input: Box<PhysicalOperator>
}