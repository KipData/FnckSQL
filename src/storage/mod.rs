pub(crate) mod memory;

use std::io;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use crate::catalog::{CatalogError, RootCatalog};
use crate::storage::memory::InMemoryStorage;
use crate::types::TableId;

#[derive(Debug)]
pub enum StorageImpl {
    InMemoryStorage(InMemoryStorage),
}

pub trait Storage: Sync + Send + 'static {
    type TableType: Table;

    fn create_table(
        &self,
        table_name: &str,
        columns: Vec<RecordBatch>,
    ) -> Result<TableId, StorageError>;
    fn get_table(&self, id: TableId) -> Result<Self::TableType, StorageError>;
    fn get_catalog(&self) -> RootCatalog;
    fn show_tables(&self) -> Result<RecordBatch, StorageError>;
}

/// Optional bounds of the reader, of the form (offset, limit).
type Bounds = Option<(usize, usize)>;
type Projections = Option<Vec<usize>>;

pub trait Table: Sync + Send + Clone + 'static {
    type TransactionType: Transaction;

    /// The bounds is applied to the whole data batches, not per batch.
    ///
    /// The projections is column indices.
    fn read(
        &self,
        bounds: Bounds,
        projection: Projections,
    ) -> Result<Self::TransactionType, StorageError>;
}

// currently we use a transaction to hold csv reader
pub trait Transaction: Sync + Send + 'static {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, StorageError>;
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("arrow error")]
    ArrowError(#[from] ArrowError),

    #[error("io error")]
    IoError(#[from] io::Error),

    #[error("table not found: {0}")]
    TableNotFound(TableId),

    #[error("catalog error")]
    CatalogError(#[from] CatalogError),
}
