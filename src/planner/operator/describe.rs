use crate::catalog::TableName;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct DescribeOperator {
    pub table_name: TableName,
}

impl fmt::Display for DescribeOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Describe {}", self.table_name)?;

        Ok(())
    }
}
