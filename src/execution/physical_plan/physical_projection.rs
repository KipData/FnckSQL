use crate::execution::physical_plan::PhysicalPlan;
use crate::expression::ScalarExpression;

#[derive(Debug)]
pub struct PhysicalProjection {
    pub(crate) exprs: Vec<ScalarExpression>,
    pub(crate) input: Box<PhysicalPlan>
}