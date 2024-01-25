use crate::storage::StorageError;
use crate::types::errors::TypeError;
use kip_db::KernelError;

/// The architecture and some components,
/// such as (/core) are referenced from sqlrs
pub mod core;
pub mod heuristic;
pub mod rule;
pub mod utils;

#[derive(thiserror::Error, Debug)]
pub enum OptimizerError {
    #[error("type error")]
    TypeError(
        #[source]
        #[from]
        TypeError,
    ),
    #[error("plan is empty")]
    EmptyPlan,
    #[error("this column must belong to a table")]
    OwnerLessColumn,
    #[error("there are more buckets than elements")]
    TooManyBuckets,
    #[error("io: {0}")]
    IO(
        #[source]
        #[from]
        std::io::Error,
    ),
    #[error("cache error: {0}")]
    Cache(
        #[source]
        #[from]
        KernelError,
    ),
    /// Serialization or deserialization error
    #[error(transparent)]
    SerdeBinCode(#[from] Box<bincode::ErrorKind>),
    #[error("storage error: {0}")]
    Storage(
        #[source]
        #[from]
        StorageError,
    ),
}
