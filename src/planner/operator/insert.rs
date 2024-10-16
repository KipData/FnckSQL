use crate::catalog::TableName;
use serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct InsertOperator {
    pub table_name: TableName,
    pub is_overwrite: bool,
}

impl fmt::Display for InsertOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Insert {}, Is Overwrite: {}",
            self.table_name, self.is_overwrite
        )?;

        Ok(())
    }
}
