use crate::execution_v1::physical_plan::PhysicalPlan;
use crate::planner::operator::sort::SortOperator;

#[derive(Debug)]
pub struct PhysicalSort {
    pub(crate) op: SortOperator,
    pub(crate) input: Box<PhysicalPlan>
}