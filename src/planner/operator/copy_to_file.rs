use crate::binder::copy::ExtSource;
use crate::types::tuple::SchemaRef;
use fnck_sql_serde_macros::ReferenceSerialization;
use itertools::Itertools;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct CopyToFileOperator {
    pub table: String,
    pub target: ExtSource,
    pub schema_ref: SchemaRef,
}

impl fmt::Display for CopyToFileOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let columns = self
            .schema_ref
            .iter()
            .map(|column| column.name().to_string())
            .join(", ");
        write!(
            f,
            "Copy {} -> {} [{}]",
            self.table,
            self.target.path.display(),
            columns
        )?;

        Ok(())
    }
}
