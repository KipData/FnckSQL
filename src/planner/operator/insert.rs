use crate::types::TableId;

#[derive(Debug, PartialEq, Clone)]
pub struct InsertOperator {
    pub table_id: TableId,
}