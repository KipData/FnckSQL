use crate::execution_v1::physical_plan::PhysicalOperator;

#[derive(Debug)]
pub struct PhysicalInsert {
    pub(crate) table_name: String,
    pub(crate) input: Box<PhysicalOperator>
}