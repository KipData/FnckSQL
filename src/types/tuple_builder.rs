use crate::catalog::ColumnCatalog;
use crate::errors::DatabaseError;
use crate::types::tuple::{SchemaRef, Tuple};
use crate::types::value::{DataValue, ValueRef};
use std::sync::Arc;

pub struct TupleBuilder<'a> {
    schema_ref: &'a SchemaRef,
}

impl<'a> TupleBuilder<'a> {
    pub fn new(schema_ref: &'a SchemaRef) -> Self {
        TupleBuilder { schema_ref }
    }

    pub fn build_result(header: String, message: String) -> Result<Tuple, DatabaseError> {
        let columns = Arc::new(vec![Arc::new(ColumnCatalog::new_dummy(header))]);
        let values = vec![Arc::new(DataValue::Utf8(Some(message)))];

        Ok(Tuple {
            id: None,
            schema_ref: columns,
            values,
        })
    }

    pub fn build(
        &self,
        id: Option<ValueRef>,
        values: Vec<ValueRef>,
    ) -> Result<Tuple, DatabaseError> {
        if values.len() != self.schema_ref.len() {
            return Err(DatabaseError::MisMatch("types", "values"));
        }

        Ok(Tuple {
            id,
            schema_ref: self.schema_ref.clone(),
            values,
        })
    }

    pub fn build_with_row<'b>(
        &self,
        row: impl IntoIterator<Item = &'b str>,
    ) -> Result<Tuple, DatabaseError> {
        let mut values = Vec::with_capacity(self.schema_ref.len());
        let mut primary_key = None;

        for (i, value) in row.into_iter().enumerate() {
            let data_value = Arc::new(
                DataValue::Utf8(Some(value.to_string())).cast(self.schema_ref[i].datatype())?,
            );

            if primary_key.is_none() && self.schema_ref[i].desc.is_primary {
                primary_key = Some(data_value.clone());
            }
            values.push(data_value);
        }
        if values.len() != self.schema_ref.len() {
            return Err(DatabaseError::MisMatch("types", "values"));
        }

        Ok(Tuple {
            id: primary_key,
            schema_ref: self.schema_ref.clone(),
            values,
        })
    }
}
