// Module: catalog
pub(crate) use self::column::*;
pub(crate) use self::database::*;
pub(crate) use self::root::*;
pub(crate) use self::schema::*;
pub(crate) use self::table::*;

use crate::types::{ColumnIdT, SchemaIdT, TableIdT};
use std::sync::Arc;

pub(crate) type ColumnCatalogRef = Arc<ColumnCatalog>;
pub(crate) type TableCatalogRef = Arc<TableCatalog>;
pub(crate) type SchemaCatalogRef = Arc<SchemaCatalog>;
pub(crate) type DatabaseCatalogRef = Arc<DatabaseCatalog>;
pub(crate) type RootCatalogRef = Arc<RootCatalog>;
/// The type of catalog reference.
pub(crate) type CatalogRef = Arc<RootCatalog>;

pub(crate) static DEFAULT_DATABASE_NAME: &str = "postgres";
pub(crate) static DEFAULT_SCHEMA_NAME: &str = "postgres";

mod column;
mod database;
mod root;
mod schema;
mod table;

/// The reference ID of a table.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct TableRefId {
    pub schema_id: SchemaIdT,
    pub table_id: TableIdT,
}

impl TableRefId {
    pub const fn new(schema_id: SchemaIdT, table_id: TableIdT) -> Self {
        TableRefId {
            schema_id,
            table_id,
        }
    }
}

/// The reference ID of a column.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct ColumnRefId {
    pub schema_id: SchemaIdT,
    pub table_id: TableIdT,
    pub column_id: ColumnIdT,
}

impl ColumnRefId {
    pub const fn from_table(table: TableRefId, column_id: ColumnIdT) -> Self {
        ColumnRefId {
            schema_id: table.schema_id,
            table_id: table.table_id,
            column_id,
        }
    }

    pub const fn new(schema_id: SchemaIdT, table_id: TableIdT, column_id: ColumnIdT) -> Self {
        ColumnRefId {
            schema_id,
            table_id,
            column_id,
        }
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
