use crate::planner::operator::scan::ScanOperator;

#[derive(Debug)]
pub struct PhysicalTableScan {
    pub(crate) base: ScanOperator
}