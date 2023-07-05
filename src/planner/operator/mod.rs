pub mod aggregate;
pub mod filter;
pub mod join;
pub mod limit;
pub mod project;
pub mod scan;
pub mod sort;

use std::sync::Arc;

use self::{
    aggregate::AggregateOperator, filter::FilterOperator, join::JoinOperator, limit::LimitOperator,
    project::ProjectOperator, scan::ScanOperator, sort::SortOperator,
};

pub type OperatorRef = Arc<Operator>;

#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    Dummy,
    Aggregate(AggregateOperator),
    Filter(FilterOperator),
    Join(JoinOperator),
    Project(ProjectOperator),
    Scan(ScanOperator),
    Sort(SortOperator),
    Limit(LimitOperator),
}
