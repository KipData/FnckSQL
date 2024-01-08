use crate::types::value::ValueRef;
use crate::types::ColumnId;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub type IndexId = u32;
pub type IndexMetaRef = Arc<IndexMeta>;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct IndexMeta {
    pub id: IndexId,
    pub column_ids: Vec<ColumnId>,
    pub name: String,
    pub is_unique: bool,
    pub is_primary: bool,
}

pub struct Index {
    pub id: IndexId,
    pub column_values: Vec<ValueRef>,
}

impl Index {
    pub fn new(id: IndexId, column_values: Vec<ValueRef>) -> Self {
        Index { id, column_values }
    }
}
