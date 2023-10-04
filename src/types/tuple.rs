use crate::catalog::ColumnRef;
use crate::types::value::{DataValue, ValueRef};
use comfy_table::{Cell, Table};
use integer_encoding::FixedInt;
use itertools::Itertools;
use std::sync::Arc;

const BITS_MAX_INDEX: usize = 8;

pub type TupleId = ValueRef;

#[derive(Clone, Debug, PartialEq)]
pub struct Tuple {
    pub id: Option<TupleId>,
    pub columns: Vec<ColumnRef>,
    pub values: Vec<ValueRef>,
}

impl Tuple {
    pub fn deserialize_from(columns: Vec<ColumnRef>, bytes: &[u8]) -> Self {
        fn is_none(bits: u8, i: usize) -> bool {
            bits & (1 << (7 - i)) > 0
        }

        let values_len = columns.len();
        let mut values = Vec::with_capacity(values_len);
        let bits_len = (values_len + BITS_MAX_INDEX) / BITS_MAX_INDEX;
        let mut id_option = None;

        let mut pos = bits_len;

        for (i, col) in columns.iter().enumerate() {
            let logic_type = col.datatype();

            if is_none(bytes[i / BITS_MAX_INDEX], i % BITS_MAX_INDEX) {
                values.push(Arc::new(DataValue::none(logic_type)));
            } else if let Some(len) = logic_type.raw_len() {
                /// fixed length (e.g.: int)
                values.push(Arc::new(DataValue::from_raw(
                    &bytes[pos..pos + len],
                    logic_type,
                )));
                pos += len;
            } else {
                /// variable length (e.g.: varchar)
                let len = u32::decode_fixed(&bytes[pos..pos + 4]) as usize;
                pos += 4;
                values.push(Arc::new(DataValue::from_raw(
                    &bytes[pos..pos + len],
                    logic_type,
                )));
                pos += len;
            }

            if col.desc.is_primary {
                id_option = Some(values[i].clone());
            }
        }

        Tuple {
            id: id_option,
            columns,
            values,
        }
    }

    /// e.g.: bits(u8)..|data_0(len for utf8_1)|utf8_0|data_1|
    /// Tips: all len is u32
    pub fn serialize_to(&self) -> Vec<u8> {
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
                let mut value_bytes = value.to_raw();

                if value.is_variable() {
                    bytes.append(&mut (value_bytes.len() as u32).encode_fixed_vec());
                }
                bytes.append(&mut value_bytes);
            }
        }

        bytes
    }
}

pub fn create_table(tuples: &[Tuple]) -> Table {
    let mut table = Table::new();

    if tuples.is_empty() {
        return table;
    }

    let mut header = Vec::new();
    for col in &tuples[0].columns {
        header.push(Cell::new(col.name.clone()));
    }
    table.set_header(header);

    for tuple in tuples {
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
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::sync::Arc;

    #[test]
    fn test_tuple_serialize_to_and_deserialize_from() {
        let columns = vec![
            Arc::new(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::UInteger, false, false),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c3".to_string(),
                false,
                ColumnDesc::new(LogicalType::Varchar(Some(2)), false, false),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c4".to_string(),
                false,
                ColumnDesc::new(LogicalType::Smallint, false, false),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c5".to_string(),
                false,
                ColumnDesc::new(LogicalType::USmallint, false, false),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c6".to_string(),
                false,
                ColumnDesc::new(LogicalType::Float, false, false),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c7".to_string(),
                false,
                ColumnDesc::new(LogicalType::Double, false, false),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c8".to_string(),
                false,
                ColumnDesc::new(LogicalType::Tinyint, false, false),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c9".to_string(),
                false,
                ColumnDesc::new(LogicalType::UTinyint, false, false),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c10".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false, false),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c11".to_string(),
                false,
                ColumnDesc::new(LogicalType::DateTime, false, false),
                None,
            )),
            Arc::new(ColumnCatalog::new(
                "c12".to_string(),
                false,
                ColumnDesc::new(LogicalType::Date, false, false),
                None,
            )),
        ];

        let tuples = vec![
            Tuple {
                id: Some(Arc::new(DataValue::Int32(Some(0)))),
                columns: columns.clone(),
                values: vec![
                    Arc::new(DataValue::Int32(Some(0))),
                    Arc::new(DataValue::UInt32(Some(1))),
                    Arc::new(DataValue::Utf8(Some("LOL".to_string()))),
                    Arc::new(DataValue::Int16(Some(1))),
                    Arc::new(DataValue::UInt16(Some(1))),
                    Arc::new(DataValue::Float32(Some(0.1))),
                    Arc::new(DataValue::Float64(Some(0.1))),
                    Arc::new(DataValue::Int8(Some(1))),
                    Arc::new(DataValue::UInt8(Some(1))),
                    Arc::new(DataValue::Boolean(Some(true))),
                    Arc::new(DataValue::Date64(Some(0))),
                    Arc::new(DataValue::Date32(Some(0))),
                ],
            },
            Tuple {
                id: Some(Arc::new(DataValue::Int32(Some(1)))),
                columns: columns.clone(),
                values: vec![
                    Arc::new(DataValue::Int32(Some(1))),
                    Arc::new(DataValue::UInt32(None)),
                    Arc::new(DataValue::Utf8(None)),
                    Arc::new(DataValue::Int16(None)),
                    Arc::new(DataValue::UInt16(None)),
                    Arc::new(DataValue::Float32(None)),
                    Arc::new(DataValue::Float64(None)),
                    Arc::new(DataValue::Int8(None)),
                    Arc::new(DataValue::UInt8(None)),
                    Arc::new(DataValue::Boolean(None)),
                    Arc::new(DataValue::Date64(None)),
                    Arc::new(DataValue::Date32(None)),
                ],
            },
        ];

        let tuple_0 = Tuple::deserialize_from(columns.clone(), &tuples[0].serialize_to());
        let tuple_1 = Tuple::deserialize_from(columns.clone(), &tuples[1].serialize_to());

        assert_eq!(tuples[0], tuple_0);
        assert_eq!(tuples[1], tuple_1);
    }
}
