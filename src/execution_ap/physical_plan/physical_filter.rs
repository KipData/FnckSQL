use crate::execution_ap::physical_plan::PhysicalPlan;
use crate::expression::ScalarExpression;

#[derive(Debug)]
pub struct PhysicalFilter {
    pub(crate) predicate: ScalarExpression,
    pub(crate) input: Box<PhysicalPlan>
}