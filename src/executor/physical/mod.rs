pub mod physical_filter;
pub mod physical_limit;
pub mod physical_plan_builder;
pub mod physical_project;
pub mod physical_scan;
pub mod physical_sort;

use self::{
    physical_filter::PhysicalFilter, physical_limit::PhysicalLimit,
    physical_project::PhysicalProject, physical_scan::PhysicalTableScan,
    physical_sort::PhysicalSort,
};

pub type PhysicalPlanBoxed = Box<PhysicalPlan>;

pub enum PhysicalPlan {
    TableScan(PhysicalTableScan),
    Prjection(PhysicalProject),
    Sort(PhysicalSort),
    Filter(PhysicalFilter),
    Limit(PhysicalLimit),
}
