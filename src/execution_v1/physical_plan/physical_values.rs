use crate::planner::operator::values::ValuesOperator;

#[derive(Debug)]
pub struct PhysicalValues {
    pub(crate) op: ValuesOperator
}