use crate::catalog::{ColumnCatalog, ColumnRef};
use crate::errors::DatabaseError;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, ValueRef};
use std::sync::Arc;

pub struct TupleBuilder {
    columns: Arc<Vec<ColumnRef>>,
}

impl TupleBuilder {
    pub fn new(columns: Vec<ColumnRef>) -> Self {
        TupleBuilder {
            columns: Arc::new(columns),
        }
    }

    pub fn build_result(header: String, message: String) -> Result<Tuple, DatabaseError> {
        let columns = Arc::new(vec![Arc::new(ColumnCatalog::new_dummy(header))]);
        let values = vec![Arc::new(DataValue::Utf8(Some(message)))];

        Ok(Tuple {
            id: None,
            columns,
            values,
        })
    }

    pub fn build(
        &self,
        id: Option<ValueRef>,
        values: Vec<ValueRef>,
    ) -> Result<Tuple, DatabaseError> {
        if values.len() != self.columns.len() {
            return Err(DatabaseError::MisMatch(
                "types".to_string(),
                "values".to_string(),
            ));
        }

        Ok(Tuple {
            id,
            columns: self.columns.clone(),
            values,
        })
    }

    pub fn build_with_row<'a>(
        &self,
        row: impl IntoIterator<Item = &'a str>,
    ) -> Result<Tuple, DatabaseError> {
        let mut values = Vec::with_capacity(self.columns.len());
        let mut primary_key = None;

        for (i, value) in row.into_iter().enumerate() {
            let data_value = Arc::new(
                DataValue::Utf8(Some(value.to_string())).cast(self.columns[i].datatype())?,
            );

            if primary_key.is_none() && self.columns[i].desc.is_primary {
                primary_key = Some(data_value.clone());
            }
            values.push(data_value);
        }
        if values.len() != self.columns.len() {
            return Err(DatabaseError::MisMatch(
                "types".to_string(),
                "values".to_string(),
            ));
        }

        Ok(Tuple {
            id: primary_key,
            columns: self.columns.clone(),
            values,
        })
    }
}
