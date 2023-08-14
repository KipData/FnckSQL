use crate::execution_v1::physical_plan::PhysicalOperator;
use crate::planner::operator::aggregate::AggregateOperator;

#[derive(Debug)]
pub struct PhysicalAgg{
    pub(crate) op: AggregateOperator,
    pub(crate) input: Box<PhysicalOperator>
}