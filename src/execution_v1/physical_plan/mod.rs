use crate::execution_v1::physical_plan::physical_projection::PhysicalProjection;
use crate::execution_v1::physical_plan::physical_table_scan::PhysicalTableScan;

pub(crate) mod physical_table_scan;
pub(crate) mod physical_projection;
pub(crate) mod physical_plan_builder;

pub enum PhysicalOperator {
    TableScan(PhysicalTableScan),
    Projection(PhysicalProjection)
}