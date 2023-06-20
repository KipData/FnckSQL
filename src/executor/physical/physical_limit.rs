use super::PhysicalPlanBoxed;

pub struct PhysicalLimit {
    pub plan_id: u32,

    pub input: PhysicalPlanBoxed,
    pub limit: usize,
    pub offset: usize,
}
