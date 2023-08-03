use crate::execution_v1::physical_plan::PhysicalPlan;
use crate::expression::ScalarExpression;

#[derive(Debug)]
pub struct PhysicalFilter {
    pub(crate) predicate: ScalarExpression,
    pub(crate) input: Box<PhysicalPlan>
}