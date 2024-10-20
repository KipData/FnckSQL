use crate::types::tuple::SchemaRef;
use crate::types::value::ValueRef;
use itertools::Itertools;
use serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct ValuesOperator {
    pub rows: Vec<Vec<ValueRef>>,
    pub schema_ref: SchemaRef,
}

impl fmt::Display for ValuesOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let columns = self
            .rows
            .iter()
            .map(|row| {
                let row_string = row.iter().map(|value| format!("{value}")).join(", ");
                format!("[{row_string}]")
            })
            .join(", ");

        write!(f, "Values {}, RowsLen: {}", columns, self.rows.len())?;

        Ok(())
    }
}
