use serde::{Deserialize, Serialize};
use crate::types::ColumnId;
use crate::types::value::ValueRef;

pub type IndexId = u32;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct IndexMeta {
    pub id: IndexId,
    pub column_ids: Vec<ColumnId>,
    pub name: String,
    pub is_unique:bool
}

pub struct Index {
    pub id: IndexId,
    pub column_values: Vec<ValueRef>,
}