use crate::types::LogicalType;
use serde::{Deserialize, Serialize};

pub mod scala;
pub mod table;

#[derive(Debug, Eq, PartialEq, Hash, Clone, Serialize, Deserialize)]
pub struct FunctionSummary {
    pub(crate) name: String,
    pub(crate) arg_types: Vec<LogicalType>,
}
