pub mod executor;
pub(crate) mod physical_plan;

use sqlparser::parser::ParserError;
use crate::binder::BindError;
use crate::catalog::CatalogError;
use crate::execution::physical_plan::MappingError;
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
    #[error("type error: {0}")]
    TypeError(
        #[source]
        #[from]
        TypeError,
    ),
    #[error("storage_ap error: {0}")]
    StorageError(
        #[source]
        #[from]
        StorageError
    ),
    #[error("bind error: {0}")]
    BindError(
        #[source]
        #[from]
        BindError
    ),
    #[error("parser error: {0}")]
    ParserError(
        #[source]
        #[from]
        ParserError
    ),
    #[error("mapping error: {0}")]
    MappingError(
        #[source]
        #[from]
        MappingError
    ),
    #[error("Internal error: {0}")]
    InternalError(String),
}
