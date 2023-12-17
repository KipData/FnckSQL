// Module: catalog
use std::sync::Arc;

pub(crate) use self::column::*;
pub(crate) use self::root::*;
pub(crate) use self::table::*;

/// The type of catalog reference.
pub type RootRef = Arc<RootCatalog>;

mod column;
mod root;
mod table;

#[derive(thiserror::Error, Debug)]
pub enum CatalogError {
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
    #[error("columns empty")]
    ColumnsEmpty,
}
