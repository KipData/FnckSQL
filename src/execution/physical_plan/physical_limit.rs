use crate::execution::physical_plan::PhysicalPlan;
use crate::planner::operator::limit::LimitOperator;

#[derive(Debug)]
pub struct PhysicalLimit {
    pub(crate) op: LimitOperator,
    pub(crate) input: Box<PhysicalPlan>
}