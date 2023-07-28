use crate::execution_v1::physical_plan::physical_create_table::PhysicalCreateTable;
use crate::execution_v1::physical_plan::physical_projection::PhysicalProjection;
use crate::execution_v1::physical_plan::physical_table_scan::PhysicalTableScan;

pub(crate) mod physical_create_table;
pub(crate) mod physical_plan_builder;
pub(crate) mod physical_projection;
pub(crate) mod physical_table_scan;

#[derive(Debug)]
pub enum PhysicalOperator {
    CreateTable(PhysicalCreateTable),
    TableScan(PhysicalTableScan),
    Projection(PhysicalProjection),
}