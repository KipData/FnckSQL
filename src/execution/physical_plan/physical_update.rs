use crate::execution::physical_plan::PhysicalPlan;
use crate::types::TableId;

#[derive(Debug)]
pub struct PhysicalUpdate {
    pub(crate) table_id: TableId,
    pub(crate) input: Box<PhysicalPlan>,
    pub(crate) values: Box<PhysicalPlan>
}