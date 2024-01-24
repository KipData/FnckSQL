use std::error::Error;
use crate::types::errors::TypeError;

/// The architecture and some components,
/// such as (/core) are referenced from sqlrs
mod core;
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
    #[error("{0}")]
    BoxError(
        #[source]
        #[from]
        Box<dyn Error>,
    ),
    #[error("plan is empty")]
    EmptyPlan,
    #[error("this column must belong to a table")]
    OwnerLessColumn,
    #[error("there are more buckets than elements")]
    TooManyBuckets,
}
