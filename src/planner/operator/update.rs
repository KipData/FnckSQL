use crate::types::TableId;

#[derive(Debug, PartialEq, Clone)]
pub struct UpdateOperator {
    pub table_id: TableId,
}