use crate::execution::physical_plan::physical_create_table::PhysicalCreateTable;
use crate::execution::physical_plan::physical_filter::PhysicalFilter;
use crate::execution::physical_plan::physical_hash_join::PhysicalHashJoin;
use crate::execution::physical_plan::physical_insert::PhysicalInsert;
use crate::execution::physical_plan::physical_limit::PhysicalLimit;
use crate::execution::physical_plan::physical_projection::PhysicalProjection;
use crate::execution::physical_plan::physical_sort::PhysicalSort;
use crate::execution::physical_plan::physical_table_scan::PhysicalTableScan;
use crate::execution::physical_plan::physical_values::PhysicalValues;

pub(crate) mod physical_create_table;
pub(crate) mod physical_plan_mapping;
pub(crate) mod physical_projection;
pub(crate) mod physical_table_scan;
pub(crate) mod physical_insert;
pub(crate) mod physical_values;
pub(crate) mod physical_filter;
pub(crate) mod physical_sort;
pub(crate) mod physical_limit;
pub(crate) mod physical_hash_join;

#[derive(Debug)]
pub enum PhysicalPlan {
    Insert(PhysicalInsert),
    CreateTable(PhysicalCreateTable),
    TableScan(PhysicalTableScan),
    Projection(PhysicalProjection),
    Filter(PhysicalFilter),
    Sort(PhysicalSort),
    Values(PhysicalValues),
    Limit(PhysicalLimit),
    HashJoin(PhysicalHashJoin),
}

#[derive(thiserror::Error, Debug)]
pub enum MappingError {
    #[error("unsupported physical plan {0}")]
    Unsupported(String),
}