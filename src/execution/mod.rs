#[cfg(feature = "codegen_execute")]
pub mod codegen;
pub mod volcano;

use crate::binder::BindError;
use crate::catalog::CatalogError;
use crate::optimizer::OptimizerError;
use crate::storage::StorageError;
use crate::types::errors::TypeError;
#[cfg(feature = "codegen_execute")]
use mlua::prelude::LuaError;
use sqlparser::parser::ParserError;

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
    #[error("storage error: {0}")]
    StorageError(
        #[source]
        #[from]
        StorageError,
    ),
    #[error("bind error: {0}")]
    BindError(
        #[source]
        #[from]
        BindError,
    ),
    #[error("optimizer error: {0}")]
    Optimizer(
        #[source]
        #[from]
        OptimizerError,
    ),
    #[error("parser error: {0}")]
    ParserError(
        #[source]
        #[from]
        ParserError,
    ),
    #[error("Internal error: {0}")]
    InternalError(String),
    #[error("io error: {0}")]
    Io(
        #[from]
        #[source]
        std::io::Error,
    ),
    #[error("csv error: {0}")]
    Csv(
        #[from]
        #[source]
        csv::Error,
    ),
    #[error("tuple length mismatch: expected {expected} but got {actual}")]
    LengthMismatch { expected: usize, actual: usize },
    #[error("join error")]
    JoinError(
        #[from]
        #[source]
        tokio::task::JoinError,
    ),
    #[cfg(feature = "codegen_execute")]
    #[error("lua error")]
    LuaError(
        #[from]
        #[source]
        LuaError,
    ),
    #[error("channel close")]
    ChannelClose,
    #[error("invalid index")]
    InvalidIndex,
}
