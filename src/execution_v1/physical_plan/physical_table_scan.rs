use crate::planner::operator::scan::ScanOperator;

pub struct PhysicalTableScan {
    pub(crate) plan_id: u32,
    pub(crate) base: ScanOperator
}