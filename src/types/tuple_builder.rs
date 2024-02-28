use crate::errors::DatabaseError;
use crate::types::tuple::{Schema, Tuple};
use crate::types::value::DataValue;
use std::sync::Arc;

pub struct TupleBuilder<'a> {
    schema: &'a Schema,
}

impl<'a> TupleBuilder<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        TupleBuilder { schema }
    }

    pub fn build_result(message: String) -> Tuple {
        let values = vec![Arc::new(DataValue::Utf8(Some(message)))];

        Tuple { id: None, values }
    }

    pub fn build_with_row<'b>(
        &self,
        row: impl IntoIterator<Item = &'b str>,
    ) -> Result<Tuple, DatabaseError> {
        let mut values = Vec::with_capacity(self.schema.len());
        let mut primary_key = None;

        for (i, value) in row.into_iter().enumerate() {
            let data_value =
                Arc::new(DataValue::Utf8(Some(value.to_string())).cast(self.schema[i].datatype())?);

            if primary_key.is_none() && self.schema[i].desc.is_primary {
                primary_key = Some(data_value.clone());
            }
            values.push(data_value);
        }
        if values.len() != self.schema.len() {
            return Err(DatabaseError::MisMatch("types", "values"));
        }

        Ok(Tuple {
            id: primary_key,
            values,
        })
    }
}
