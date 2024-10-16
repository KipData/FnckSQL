use crate::binder::copy::ExtSource;
use crate::types::tuple::SchemaRef;
use itertools::Itertools;
use serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct CopyFromFileOperator {
    pub table: String,
    pub source: ExtSource,
    pub schema_ref: SchemaRef,
}

impl fmt::Display for CopyFromFileOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let columns = self
            .schema_ref
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
