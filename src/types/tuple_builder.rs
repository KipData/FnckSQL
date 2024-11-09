use crate::errors::DatabaseError;
use crate::types::tuple::{Schema, Tuple, TupleId};
use crate::types::value::{DataValue, Utf8Type};
use itertools::Itertools;
use sqlparser::ast::CharLengthUnits;

pub(crate) struct TupleIdBuilder {
    primary_indexes: Vec<usize>,
    tmp_keys: Vec<Option<DataValue>>,
}

pub struct TupleBuilder<'a> {
    schema: &'a Schema,
}

impl TupleIdBuilder {
    pub(crate) fn new(schema: &Schema) -> Self {
        let primary_indexes = schema
            .iter()
            .filter_map(|column| column.desc().primary())
            .enumerate()
            .sorted_by_key(|(_, p_i)| *p_i)
            .map(|(i, _)| i)
            .collect_vec();
        let tmp_keys = Vec::with_capacity(primary_indexes.len());
        Self {
            primary_indexes,
            tmp_keys,
        }
    }

    pub(crate) fn append(&mut self, value: DataValue) {
        self.tmp_keys.push(Some(value));
    }

    pub(crate) fn build(&mut self) -> Option<TupleId> {
        (!self.tmp_keys.is_empty()).then(|| {
            if self.tmp_keys.len() == 1 {
                self.tmp_keys.pop().unwrap().unwrap()
            } else {
                let mut primary_keys = Vec::new();

                for i in self.primary_indexes.iter() {
                    primary_keys.push(self.tmp_keys[*i].take().unwrap());
                }
                self.tmp_keys.clear();

                DataValue::Tuple(Some(primary_keys))
            }
        })
    }
}

impl<'a> TupleBuilder<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        TupleBuilder { schema }
    }

    pub fn build_result(message: String) -> Tuple {
        let values = vec![DataValue::Utf8 {
            value: Some(message),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        }];

        Tuple { id: None, values }
    }

    pub fn build_with_row<'b>(
        &self,
        row: impl IntoIterator<Item = &'b str>,
    ) -> Result<Tuple, DatabaseError> {
        let mut values = Vec::with_capacity(self.schema.len());
        let mut id_builder = TupleIdBuilder::new(self.schema);

        for (i, value) in row.into_iter().enumerate() {
            let data_value = DataValue::Utf8 {
                value: Some(value.to_string()),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            }
            .cast(self.schema[i].datatype())?;

            if self.schema[i].desc().is_primary() {
                id_builder.append(data_value.clone());
            }
            values.push(data_value);
        }
        if values.len() != self.schema.len() {
            return Err(DatabaseError::MisMatch("types", "values"));
        }

        Ok(Tuple {
            id: id_builder.build(),
            values,
        })
    }
}
