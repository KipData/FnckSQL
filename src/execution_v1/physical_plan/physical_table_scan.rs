use crate::planner::operator::scan::ScanOperator;

#[derive(Debug)]
pub struct PhysicalTableScan {
    pub(crate) plan_id: u32,
    pub(crate) base: ScanOperator
}