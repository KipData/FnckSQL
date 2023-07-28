use crate::planner::operator::insert::InsertOperator;

#[derive(Debug)]
pub struct PhysicalInsert {
    pub op: InsertOperator
}