use crate::expression::ScalarExpression;

use super::PhysicalPlanBoxed;

pub struct PhysicalFilter {
    pub plan_id: u32,
    pub input: PhysicalPlanBoxed,
    pub predicates: ScalarExpression,
}
