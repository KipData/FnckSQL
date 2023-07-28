use crate::planner::operator::create_table::CreateTableOperator;

#[derive(Debug)]
pub struct PhysicalCreateTable {
    pub op: CreateTableOperator
}
