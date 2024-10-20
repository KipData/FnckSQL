use crate::catalog::TableName;
use serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct UpdateOperator {
    pub table_name: TableName,
}

impl fmt::Display for UpdateOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Update {}", self.table_name)?;

        Ok(())
    }
}
