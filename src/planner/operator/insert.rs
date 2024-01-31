use crate::catalog::TableName;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
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
