use crate::planner::operator::scan::ScanOperator;

pub struct PhysicalTableScan {
    pub plan_id: u32,
    pub operator: ScanOperator,
}
