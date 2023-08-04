use crate::execution_v1::physical_plan::PhysicalPlan;
use crate::planner::operator::join::JoinOperator;

#[derive(Debug)]
pub struct PhysicalHashJoin {
    pub(crate) op: JoinOperator,
    pub(crate) left_input: Box<PhysicalPlan>,
    pub(crate) right_input: Box<PhysicalPlan>
}