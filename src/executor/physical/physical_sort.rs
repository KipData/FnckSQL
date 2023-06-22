use crate::planner::operator::sort::SortField;

use super::PhysicalPlanBoxed;
// use crate::planner::operator::logical_sort::SortField;

pub struct PhysicalSort {
    pub plan_id: u32,
    pub input: PhysicalPlanBoxed,
    pub order_by: Vec<SortField>,
    pub limit: Option<usize>,
}
