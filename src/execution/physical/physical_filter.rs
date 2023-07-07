use crate::expression::ScalarExpression;

use super::PhysicalOperatorRef;

#[derive(Debug)]
pub struct PhysicalFilter {
    pub plan_id: u32,
    pub input: PhysicalOperatorRef,
    pub predicates: ScalarExpression,
}
