use crate::execution::physical_plan::PhysicalPlan;

#[derive(Debug)]
pub struct PhysicalInsert {
    pub(crate) table_name: String,
    pub(crate) input: Box<PhysicalPlan>
}