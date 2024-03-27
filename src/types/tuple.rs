use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::types::value::{DataValue, ValueRef};
use crate::types::LogicalType;
use comfy_table::{Cell, Table};
use integer_encoding::FixedInt;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::sync::Arc;

lazy_static! {
    pub static ref EMPTY_TUPLE: Tuple = {
        Tuple {
            id: None,
            values: vec![],
        }
    };
}

const BITS_MAX_INDEX: usize = 8;

pub type TupleId = ValueRef;
pub type Schema = Vec<ColumnRef>;
pub type SchemaRef = Arc<Schema>;

pub fn types(schema: &Schema) -> Vec<LogicalType> {
    schema.iter().map(|column| *column.datatype()).collect_vec()
}

#[derive(Clone, Debug, PartialEq)]
pub struct Tuple {
    pub id: Option<TupleId>,
    pub values: Vec<ValueRef>,
}

impl Tuple {
    pub fn deserialize_from(
        table_types: &[LogicalType],
        projections: &[usize],
        schema: &Schema,
        bytes: &[u8],
    ) -> Self {
        assert!(!schema.is_empty());
        assert_eq!(projections.len(), schema.len());

        fn is_none(bits: u8, i: usize) -> bool {
            bits & (1 << (7 - i)) > 0
        }

        let values_len = schema.len();
        let mut tuple_values = Vec::with_capacity(values_len);
        let bits_len = (values_len + BITS_MAX_INDEX) / BITS_MAX_INDEX;
        let mut id_option = None;

        let mut projection_i = 0;
        let mut pos = bits_len;

        for (i, logic_type) in table_types.iter().enumerate() {
            if projection_i >= values_len {
                break;
            }
            if is_none(bytes[i / BITS_MAX_INDEX], i % BITS_MAX_INDEX) {
                if projections[projection_i] == i {
                    tuple_values.push(Arc::new(DataValue::none(logic_type)));
                    Self::values_push(schema, &tuple_values, &mut id_option, &mut projection_i);
                }
            } else if let Some(len) = logic_type.raw_len() {
                /// fixed length (e.g.: int)
                if projections[projection_i] == i {
                    tuple_values.push(Arc::new(DataValue::from_raw(
                        &bytes[pos..pos + len],
                        logic_type,
                    )));
                    Self::values_push(schema, &tuple_values, &mut id_option, &mut projection_i);
                }
                pos += len;
            } else {
                /// variable length (e.g.: varchar)
                let len = u32::decode_fixed(&bytes[pos..pos + 4]) as usize;
                pos += 4;
                if projections[projection_i] == i {
                    tuple_values.push(Arc::new(DataValue::from_raw(
                        &bytes[pos..pos + len],
                        logic_type,
                    )));
                    Self::values_push(schema, &tuple_values, &mut id_option, &mut projection_i);
                }
                pos += len;
            }
        }

        Tuple {
            id: id_option,
            values: tuple_values,
        }
    }

    fn values_push(
        tuple_columns: &Schema,
        tuple_values: &[ValueRef],
        id_option: &mut Option<Arc<DataValue>>,
        projection_i: &mut usize,
    ) {
        if tuple_columns[*projection_i].desc.is_primary {
            let _ = id_option.replace(tuple_values[*projection_i].clone());
        }
        *projection_i += 1;
    }

    /// e.g.: bits(u8)..|data_0(len for utf8_1)|utf8_0|data_1|
    /// Tips: all len is u32
    pub fn serialize_to(&self, types: &[LogicalType]) -> Result<Vec<u8>, DatabaseError> {
        assert_eq!(self.values.len(), types.len());

        fn flip_bit(bits: u8, i: usize) -> u8 {
            bits | (1 << (7 - i))
        }

        let values_len = self.values.len();
        let bits_len = (values_len + BITS_MAX_INDEX) / BITS_MAX_INDEX;
        let mut bytes = vec![0_u8; bits_len];

        for (i, value) in self.values.iter().enumerate() {
            if value.is_null() {
                bytes[i / BITS_MAX_INDEX] = flip_bit(bytes[i / BITS_MAX_INDEX], i % BITS_MAX_INDEX);
            } else {
                let logical_type = types[i];
                let value_len = value.to_raw(&mut bytes)?;

                if logical_type.raw_len().is_none() {
                    let index = bytes.len() - value_len;

                    bytes.splice(index..index, (value_len as u32).encode_fixed_vec());
                }
            }
        }

        Ok(bytes)
    }
}

pub fn create_table(schema: &Schema, tuples: &[Tuple]) -> Table {
    let mut table = Table::new();

    if tuples.is_empty() {
        return table;
    }

    let mut header = Vec::new();
    for col in schema.iter() {
        header.push(Cell::new(col.full_name()));
    }
    table.set_header(header);

    for tuple in tuples {
        assert_eq!(schema.len(), tuple.values.len());

        let cells = tuple
            .values
            .iter()
            .map(|value| Cell::new(format!("{value}")))
            .collect_vec();

        table.add_row(cells);
    }

    table
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::types::tuple::Tuple;
    use crate::types::value::{DataValue, Utf8Type};
    use crate::types::LogicalType;
    use itertools::Itertools;
    use rust_decimal::Decimal;
    use std::sync::Arc;
    use sqlparser::ast::CharLengthUnits;

    #[test]
    fn test_tuple_serialize_to_and_deserialize_from() {
        let columns = Arc::new(vec![
            Arc::new(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::UInteger, false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c3".to_string(),
                false,
                ColumnDesc::new(LogicalType::Varchar(Some(2), CharLengthUnits::Characters), false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c4".to_string(),
                false,
                ColumnDesc::new(LogicalType::Smallint, false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c5".to_string(),
                false,
                ColumnDesc::new(LogicalType::USmallint, false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c6".to_string(),
                false,
                ColumnDesc::new(LogicalType::Float, false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c7".to_string(),
                false,
                ColumnDesc::new(LogicalType::Double, false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c8".to_string(),
                false,
                ColumnDesc::new(LogicalType::Tinyint, false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c9".to_string(),
                false,
                ColumnDesc::new(LogicalType::UTinyint, false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c10".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c11".to_string(),
                false,
                ColumnDesc::new(LogicalType::DateTime, false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c12".to_string(),
                false,
                ColumnDesc::new(LogicalType::Date, false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c13".to_string(),
                false,
                ColumnDesc::new(LogicalType::Decimal(None, None), false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c14".to_string(),
                false,
                ColumnDesc::new(LogicalType::Char(1, CharLengthUnits::Characters), false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c15".to_string(),
                false,
                ColumnDesc::new(LogicalType::Varchar(Some(2), CharLengthUnits::Octets), false, false, None),
            )),
            Arc::new(ColumnCatalog::new(
                "c16".to_string(),
                false,
                ColumnDesc::new(LogicalType::Char(1, CharLengthUnits::Octets), false, false, None),
            )),
        ]);

        let tuples = vec![
            Tuple {
                id: Some(Arc::new(DataValue::Int32(Some(0)))),
                values: vec![
                    Arc::new(DataValue::Int32(Some(0))),
                    Arc::new(DataValue::UInt32(Some(1))),
                    Arc::new(DataValue::Utf8 {
                        value: Some("LOL".to_string()),
                        ty: Utf8Type::Variable(Some(2)),
                        unit: CharLengthUnits::Characters,
                    }),
                    Arc::new(DataValue::Int16(Some(1))),
                    Arc::new(DataValue::UInt16(Some(1))),
                    Arc::new(DataValue::Float32(Some(0.1))),
                    Arc::new(DataValue::Float64(Some(0.1))),
                    Arc::new(DataValue::Int8(Some(1))),
                    Arc::new(DataValue::UInt8(Some(1))),
                    Arc::new(DataValue::Boolean(Some(true))),
                    Arc::new(DataValue::Date64(Some(0))),
                    Arc::new(DataValue::Date32(Some(0))),
                    Arc::new(DataValue::Decimal(Some(Decimal::new(0, 3)))),
                    Arc::new(DataValue::Utf8 {
                        value: Some("K".to_string()),
                        ty: Utf8Type::Fixed(1),
                        unit: CharLengthUnits::Characters,
                    }),
                    Arc::new(DataValue::Utf8 {
                        value: Some("LOL".to_string()),
                        ty: Utf8Type::Variable(Some(2)),
                        unit: CharLengthUnits::Octets,
                    }),
                    Arc::new(DataValue::Utf8 {
                        value: Some("K".to_string()),
                        ty: Utf8Type::Fixed(1),
                        unit: CharLengthUnits::Octets,
                    }),
                ],
            },
            Tuple {
                id: Some(Arc::new(DataValue::Int32(Some(1)))),
                values: vec![
                    Arc::new(DataValue::Int32(Some(1))),
                    Arc::new(DataValue::UInt32(None)),
                    Arc::new(DataValue::Utf8 {
                        value: None,
                        ty: Utf8Type::Variable(Some(2)),
                        unit: CharLengthUnits::Characters,
                    }),
                    Arc::new(DataValue::Int16(None)),
                    Arc::new(DataValue::UInt16(None)),
                    Arc::new(DataValue::Float32(None)),
                    Arc::new(DataValue::Float64(None)),
                    Arc::new(DataValue::Int8(None)),
                    Arc::new(DataValue::UInt8(None)),
                    Arc::new(DataValue::Boolean(None)),
                    Arc::new(DataValue::Date64(None)),
                    Arc::new(DataValue::Date32(None)),
                    Arc::new(DataValue::Decimal(None)),
                    Arc::new(DataValue::Utf8 {
                        value: None,
                        ty: Utf8Type::Fixed(1),
                        unit: CharLengthUnits::Characters,
                    }),
                    Arc::new(DataValue::Utf8 {
                        value: None,
                        ty: Utf8Type::Variable(Some(2)),
                        unit: CharLengthUnits::Octets,
                    }),
                    Arc::new(DataValue::Utf8 {
                        value: None,
                        ty: Utf8Type::Fixed(1),
                        unit: CharLengthUnits::Octets,
                    }),
                ],
            },
        ];
        let types = columns
            .iter()
            .map(|column| *column.datatype())
            .collect_vec();
        let columns = Arc::new(columns);

        let tuple_0 = Tuple::deserialize_from(
            &types,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            &columns,
            &tuples[0].serialize_to(&types).unwrap(),
        );
        let tuple_1 = Tuple::deserialize_from(
            &types,
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            &columns,
            &tuples[1].serialize_to(&types).unwrap(),
        );

        assert_eq!(tuples[0], tuple_0);
        assert_eq!(tuples[1], tuple_1);
    }
}
