use crate::types::errors::TypeError;

/// The architecture and some components,
/// such as (/core) are referenced from sqlrs

mod core;
pub mod heuristic;
pub mod rule;

#[derive(thiserror::Error, Debug)]
pub enum OptimizerError {
    #[error("type error")]
    TypeError(
        #[source]
        #[from]
        TypeError
    )
}