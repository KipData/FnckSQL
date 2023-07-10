// Module: catalog
pub(crate) use self::column::*;
pub(crate) use self::root::*;
pub(crate) use self::table::*;

use crate::types::{ColumnId, TableId};
use std::sync::Arc;

/// The type of catalog reference.
pub type CatalogRef = Arc<Root>;
pub(crate) type TableRef = Arc<Table>;
pub(crate) type ColumnRef = Arc<Column>;

pub(crate) static DEFAULT_DATABASE_NAME: &str = "kipsql";
pub(crate) static DEFAULT_SCHEMA_NAME: &str = "kipsql";

mod column;
mod root;
mod table;

/// The reference ID of a column.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct ColumnRefId {
    pub table_id: TableId,
    pub column_id: ColumnId,
}

impl ColumnRefId {
    pub const fn from_table(table_id: TableId, column_id: ColumnId) -> Self {
        ColumnRefId {
            table_id,
            column_id,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct TableRefId {
    pub table_id: TableId,
}

impl TableRefId {
    pub const fn new(table_id: TableId) -> Self {
        TableRefId { table_id }
    }
}

/// The error type of catalog operations.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum CatalogError {
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
}
