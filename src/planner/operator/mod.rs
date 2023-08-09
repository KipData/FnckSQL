pub mod aggregate;
pub mod create_table;
pub mod filter;
pub mod join;
pub mod limit;
pub mod project;
pub mod scan;
pub mod sort;
pub mod insert;
pub mod values;

use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::values::ValuesOperator;

use self::{
    aggregate::AggregateOperator, filter::FilterOperator, join::JoinOperator, limit::LimitOperator,
    project::ProjectOperator, scan::ScanOperator, sort::SortOperator,
};

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
    Insert(InsertOperator),
    Values(ValuesOperator),
    CreateTable(CreateTableOperator)
}
