use super::PhysicalOperatorRef;
#[derive(Debug)]
pub struct PhysicalProjection {
    pub plan_id: u32,

    pub input: PhysicalOperatorRef,
}
