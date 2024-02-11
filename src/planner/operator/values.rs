use crate::types::tuple::SchemaRef;
use crate::types::value::ValueRef;
use itertools::Itertools;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct ValuesOperator {
    pub rows: Vec<Vec<ValueRef>>,
    pub schema_ref: SchemaRef,
}

impl fmt::Display for ValuesOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let columns = self
            .schema_ref
            .iter()
            .map(|column| column.name().to_string())
            .join(", ");

        write!(f, "Values [{}], RowsLen: {}", columns, self.rows.len())?;

        Ok(())
    }
}
