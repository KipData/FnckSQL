// Module: catalog
use std::sync::Arc;

pub(crate) use self::column::*;
pub(crate) use self::root::*;
pub(crate) use self::table::*;
use crate::types::{ColumnIdx, TableIdx};

/// The type of catalog reference.
pub type CatalogRef = Arc<RootCatalog>;

pub(crate) static DEFAULT_DATABASE_NAME: &str = "kipsql";
pub(crate) static DEFAULT_SCHEMA_NAME: &str = "kipsql";

mod column;
mod root;
mod table;

/// The reference ID of a column.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct ColumnRefId {
    pub table_id: TableIdx,
    pub column_id: ColumnIdx,
}

impl ColumnRefId {
    pub const fn from_table(table_id: TableIdx, column_id: ColumnIdx) -> Self {
        ColumnRefId {
            table_id,
            column_id,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct TableRefId {
    pub table_id: TableIdx,
}

impl TableRefId {
    pub const fn new(table_id: TableIdx) -> Self {
        TableRefId { table_id }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CatalogError {
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
}
