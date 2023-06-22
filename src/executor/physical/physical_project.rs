use super::PhysicalPlanBoxed;

pub struct PhysicalProject {
    pub plan_id: u32,
    pub input: PhysicalPlanBoxed,
}
