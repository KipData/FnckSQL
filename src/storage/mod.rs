pub mod memory;
mod table_codec;
mod kip;

use async_trait::async_trait;
use kip_db::KernelError;
use crate::catalog::{CatalogError, ColumnCatalog, TableCatalog, TableName};
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;

#[async_trait]
pub trait Storage: Sync + Send + Clone + 'static {
    type TableType: Table;

    async fn create_table(
        &self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>
    ) -> Result<TableName, StorageError>;

    async fn table(&self, name: &String) -> Option<Self::TableType>;
    async fn table_catalog(&self, name: &String) -> Option<&TableCatalog>;
}

/// Optional bounds of the reader, of the form (offset, limit).
pub(crate) type Bounds = (Option<usize>, Option<usize>);
type Projections = Vec<ScalarExpression>;

pub trait Table: Sync + Send + 'static {
    type TransactionType<'a>: Transaction;

    /// The bounds is applied to the whole data batches, not per batch.
    ///
    /// The projections is column indices.
    fn read(
        &self,
        bounds: Bounds,
        projection: Projections,
    ) -> Result<Self::TransactionType<'_>, StorageError>;

    fn append(&mut self, tuple: Tuple) -> Result<(), StorageError>;
}

pub trait Transaction: Sync + Send {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, StorageError>;
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("catalog error")]
    CatalogError(#[from] CatalogError),

    #[error("kipdb error")]
    KipDBError(KernelError),
}

impl From<KernelError> for StorageError {
    fn from(value: KernelError) -> Self {
        StorageError::KipDBError(value)
    }
}