pub(crate) mod volcano_executor;
pub(crate) mod physical_plan;

use arrow::error::ArrowError;
use crate::catalog::CatalogError;
use crate::storage::StorageError;
use crate::types::errors::TypeError;

#[derive(thiserror::Error, Debug)]
pub enum ExecutorError {
    #[error("catalog error: {0}")]
    CatalogError(
        #[source]
        #[from]
        CatalogError,
    ),
    #[error("arrow error: {0}")]
    ArrowError(
        #[source]
        #[from]
        ArrowError,
    ),
    #[error("type error: {0}")]
    TypeError(
        #[source]
        #[from]
        TypeError,
    ),
    #[error("storage error: {0}")]
    StorageError(
        #[source]
        #[from]
        StorageError
    ),
    #[error("Internal error: {0}")]
    InternalError(String),
}