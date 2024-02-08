use crate::binder::copy::ExtSource;
use crate::catalog::ColumnRef;
use itertools::Itertools;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct CopyFromFileOperator {
    pub table: String,
    pub source: ExtSource,
    pub columns: Vec<ColumnRef>,
}

impl fmt::Display for CopyFromFileOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let columns = self
            .columns
            .iter()
            .map(|column| column.name().to_string())
            .join(", ");
        write!(
            f,
            "Copy {} -> {} [{}]",
            self.source.path.display(),
            self.table,
            columns
        )?;

        Ok(())
    }
}
