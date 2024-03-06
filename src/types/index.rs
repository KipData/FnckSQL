use crate::catalog::TableName;
use crate::expression::range_detacher::Range;
use crate::types::value::ValueRef;
use crate::types::{ColumnId, LogicalType};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

pub type IndexId = u32;
pub type IndexMetaRef = Arc<IndexMeta>;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct IndexInfo {
    pub(crate) meta: IndexMetaRef,
    pub(crate) range: Option<Range>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct IndexMeta {
    pub id: IndexId,
    pub column_ids: Vec<ColumnId>,
    pub table_name: TableName,
    pub pk_ty: LogicalType,
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

impl fmt::Display for IndexInfo {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.meta)?;
        write!(f, " => ")?;

        if let Some(range) = &self.range {
            write!(f, "{}", range)?;
        } else {
            write!(f, "EMPTY")?;
        }

        Ok(())
    }
}

impl fmt::Display for IndexMeta {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}
