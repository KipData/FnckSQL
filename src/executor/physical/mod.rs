pub mod physical_filter;
pub mod physical_limit;
pub mod physical_plan_builder;
pub mod physical_projection;
pub mod physical_scan;
pub mod physical_sort;
pub mod physical_result_collector;
pub mod physical_topn;

use std::sync::Arc;

use self::{
    physical_filter::PhysicalFilter, physical_limit::PhysicalLimit,
    physical_projection::PhysicalProjection, physical_scan::PhysicalTableScan,
    physical_sort::PhysicalSort,
};

pub type PhysicalOperatorRef = Arc<PhysicalOperator>;

#[derive(Debug)]
pub enum PhysicalOperator {
    TableScan(PhysicalTableScan),
    Prjection(PhysicalProjection),
    Sort(PhysicalSort),
    Filter(PhysicalFilter),
    Limit(PhysicalLimit),
    Join,
}

impl PhysicalOperator {
    pub fn is_sink(&self) -> bool {
        match self {
            PhysicalOperator::TableScan(_) => false,
            PhysicalOperator::Prjection(_) => false,
            PhysicalOperator::Sort(_) => true,
            PhysicalOperator::Filter(_) => false,
            PhysicalOperator::Limit(_) => true,
            PhysicalOperator::Join => true,
        }
    }

    pub fn parallel_sink(&self) -> bool {
        todo!()
    }

    pub fn children(&self) -> Vec<PhysicalOperatorRef> {
        match self {
            PhysicalOperator::TableScan(_) => vec![],
            PhysicalOperator::Prjection(project) => vec![project.input.clone()],
            PhysicalOperator::Sort(sort) => vec![sort.input.clone()],
            PhysicalOperator::Filter(filter) => vec![filter.input.clone()],
            PhysicalOperator::Limit(limit) => vec![limit.input.clone()],
            PhysicalOperator::Join => vec![],
        }
    }
}
