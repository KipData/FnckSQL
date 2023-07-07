use crate::planner::operator::sort::SortField;

use super::PhysicalOperatorRef;

#[derive(Debug)]
pub struct PhysicalSort {
    pub plan_id: u32,
    pub input: PhysicalOperatorRef,
    pub order_by: Vec<SortField>,
    pub limit: Option<usize>,
}
