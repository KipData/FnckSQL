use crate::catalog::TableName;
use crate::execution::physical_plan::PhysicalPlan;

#[derive(Debug)]
pub struct PhysicalDelete {
    pub(crate) table_name: TableName,
    pub(crate) input: Box<PhysicalPlan>,
}