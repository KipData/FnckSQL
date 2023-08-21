pub mod memory;

use crate::catalog::{CatalogError, ColumnCatalog, RootCatalog, TableCatalog};
use crate::expression::ScalarExpression;
use crate::types::TableId;
use crate::types::tuple::Tuple;

#[derive(Debug)]
pub enum StorageImpl {
}

pub trait Storage: Sync + Send + Clone + 'static {
    type TableType: Table;

    fn create_table(
        &self,
        table_name: String,
        columns: Vec<ColumnCatalog>
    ) -> Result<TableId, StorageError>;
    fn get_table(&self, id: &TableId) -> Result<Self::TableType, StorageError>;
    fn get_catalog(&self) -> RootCatalog;
    fn show_tables(&self) -> Result<Vec<TableCatalog>, StorageError>;
}

/// Optional bounds of the reader, of the form (offset, limit).
pub(crate) type Bounds = (Option<usize>, Option<usize>);
type Projections = Vec<ScalarExpression>;

pub trait Table: Sync + Send + Clone + 'static {
    type TransactionType<'a>: Transaction;

    /// The bounds is applied to the whole data batches, not per batch.
    ///
    /// The projections is column indices.
    fn read(
        &self,
        bounds: Bounds,
        projection: Projections,
    ) -> Result<Self::TransactionType<'_>, StorageError>;

    fn append(&self, tuple: Tuple) -> Result<(), StorageError>;
}

pub trait Transaction: Sync + Send {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, StorageError>;
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("table not found: {0}")]
    TableNotFound(TableId),

    #[error("catalog error")]
    CatalogError(#[from] CatalogError),
}