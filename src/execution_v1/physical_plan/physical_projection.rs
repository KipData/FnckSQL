use crate::execution_v1::physical_plan::PhysicalOperator;
use crate::expression::ScalarExpression;

pub struct PhysicalProjection {
    pub(crate) plan_id: u32,
    pub(crate) exprs: Vec<ScalarExpression>,
    pub(crate) input: Box<PhysicalOperator>
}