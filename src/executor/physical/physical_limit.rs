use super::PhysicalOperatorRef;

#[derive(Debug)]
pub struct PhysicalLimit {
    pub plan_id: u32,

    pub input: PhysicalOperatorRef,
    pub limit: usize,
    pub offset: usize,
}
