use crate::errors::DatabaseError;
use crate::types::tuple::{Schema, Tuple};
use crate::types::value::{DataValue, Utf8Type};
use sqlparser::ast::CharLengthUnits;
use std::sync::Arc;

pub struct TupleBuilder<'a> {
    schema: &'a Schema,
}

impl<'a> TupleBuilder<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        TupleBuilder { schema }
    }

    pub fn build_result(message: String) -> Tuple {
        let values = vec![Arc::new(DataValue::Utf8 {
            value: Some(message),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        })];

        Tuple { id: None, values }
    }

    pub fn build_with_row<'b>(
        &self,
        row: impl IntoIterator<Item = &'b str>,
    ) -> Result<Tuple, DatabaseError> {
        let mut values = Vec::with_capacity(self.schema.len());
        let mut primary_keys = Vec::new();

        for (i, value) in row.into_iter().enumerate() {
            let data_value = Arc::new(
                DataValue::Utf8 {
                    value: Some(value.to_string()),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }
                .cast(self.schema[i].datatype())?,
            );

            if self.schema[i].desc().is_primary() {
                primary_keys.push(data_value.clone());
            }
            values.push(data_value);
        }
        if values.len() != self.schema.len() {
            return Err(DatabaseError::MisMatch("types", "values"));
        }
        let id = (!primary_keys.is_empty()).then(|| {
            if primary_keys.len() == 1 {
                primary_keys.pop().unwrap()
            } else {
                Arc::new(DataValue::Tuple(Some(primary_keys)))
            }
        });

        Ok(Tuple { id, values })
    }
}
