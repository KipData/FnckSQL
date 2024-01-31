use crate::catalog::TableName;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct UpdateOperator {
    pub table_name: TableName,
}

impl fmt::Display for UpdateOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Update {}", self.table_name)?;

        Ok(())
    }
}
