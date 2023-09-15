pub mod memory;
mod table_codec;
pub mod kip;

use async_trait::async_trait;
use kip_db::error::CacheError;
use kip_db::KernelError;
use crate::catalog::{CatalogError, ColumnCatalog, TableCatalog, TableName};
use crate::expression::ScalarExpression;
use crate::types::errors::TypeError;
use crate::types::tuple::{Tuple, TupleId};

#[async_trait]
pub trait Storage: Sync + Send + Clone + 'static {
    type TableType: Table;

    async fn create_table(
        &self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>
    ) -> Result<TableName, StorageError>;

    async fn drop_table(&self, name: &String) -> Result<(), StorageError>;
    async fn drop_data(&self, name: &String) -> Result<(), StorageError>;

    async fn table(&self, name: &String) -> Option<Self::TableType>;
    async fn table_catalog(&self, name: &String) -> Option<&TableCatalog>;
}

/// Optional bounds of the reader, of the form (offset, limit).
pub(crate) type Bounds = (Option<usize>, Option<usize>);
type Projections = Vec<ScalarExpression>;

#[async_trait]
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

    fn append(&mut self, tuple: Tuple, is_overwrite: bool) -> Result<(), StorageError>;

    fn delete(&mut self, tuple_id: TupleId) -> Result<(), StorageError>;

    async fn commit(self) -> Result<(), StorageError>;
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

    #[error("cache error")]
    CacheError(CacheError),

    #[error("type error")]
    TypeError(#[from] TypeError),

    #[error("The same primary key data already exists")]
    DuplicatePrimaryKey,
}

impl From<KernelError> for StorageError {
    fn from(value: KernelError) -> Self {
        StorageError::KipDBError(value)
    }
}

impl From<CacheError> for StorageError {
    fn from(value: CacheError) -> Self {
        StorageError::CacheError(value)
    }
}