use crate::catalog::{ColumnRef, TableName};
use serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct DeleteOperator {
    pub table_name: TableName,
    // for column pruning
    pub primary_key_column: ColumnRef,
}

impl fmt::Display for DeleteOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Delete {}", self.table_name)?;

        Ok(())
    }
}
