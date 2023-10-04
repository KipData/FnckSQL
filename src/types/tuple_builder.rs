use crate::catalog::{ColumnCatalog, ColumnRef};
use crate::types::errors::TypeError;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, ValueRef};
use crate::types::LogicalType;
use std::collections::HashMap;
use std::sync::Arc;

pub struct TupleBuilder {
    data_types: Vec<LogicalType>,
    data_values: Vec<ValueRef>,
    columns: Vec<ColumnRef>,
}

impl TupleBuilder {
    pub fn new(data_types: Vec<LogicalType>, columns: Vec<ColumnRef>) -> Self {
        TupleBuilder {
            data_types,
            data_values: Vec::new(),
            columns,
        }
    }

    pub fn new_result() -> Self {
        TupleBuilder {
            data_types: Vec::new(),
            data_values: Vec::new(),
            columns: Vec::new(),
        }
    }

    pub fn push_result(self, header: &str, message: &str) -> Result<Tuple, TypeError> {
        let columns: Vec<ColumnRef> = vec![Arc::new(ColumnCatalog::new_dummy(header.to_string()))];
        let values: Vec<ValueRef> = vec![Arc::new(DataValue::Utf8(Some(String::from(message))))];
        let t = Tuple {
            id: None,
            columns,
            values,
        };
        Ok(t)
    }

    pub fn push_str_row<'a>(
        &mut self,
        row: impl IntoIterator<Item = &'a str>,
    ) -> Result<Option<Tuple>, TypeError> {
        let mut primary_key_index = None;
        let columns = self.columns.clone();
        let mut tuple_map = HashMap::new();

        for (i, value) in row.into_iter().enumerate() {
            let data_value = DataValue::Utf8(Some(value.to_string()));
            let cast_data_value = data_value.cast(&self.data_types[i])?;
            self.data_values.push(Arc::new(cast_data_value.clone()));
            let col = &columns[i];
            col.id
                .map(|col_id| tuple_map.insert(col_id, Arc::new(cast_data_value.clone())));
            if col.desc.is_primary {
                primary_key_index = Some(i);
                break;
            }
        }

        let primary_col_id = primary_key_index
            .map(|i| columns[i].id.unwrap())
            .ok_or_else(|| TypeError::PrimaryKeyNotFound)?;

        let tuple_id = tuple_map
            .get(&primary_col_id)
            .ok_or_else(|| TypeError::PrimaryKeyNotFound)?
            .clone();

        let tuple = if self.data_values.len() == self.data_types.len() {
            Some(Tuple {
                id: Some(tuple_id),
                columns: self.columns.clone(),
                values: self.data_values.clone(),
            })
        } else {
            None
        };
        Ok(tuple)
    }
}
