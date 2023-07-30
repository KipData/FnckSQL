use crate::execution_v1::physical_plan::PhysicalOperator;
use crate::expression::ScalarExpression;

#[derive(Debug)]
pub struct PhysicalFilter {
    pub(crate) plan_id: u32,
    pub(crate) predicate: ScalarExpression,
    pub(crate) input: Box<PhysicalOperator>
}