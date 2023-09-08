use std::sync::Arc;
use comfy_table::{Cell, Table};
use integer_encoding::FixedInt;
use itertools::Itertools;
use crate::catalog::ColumnRef;
use crate::types::value::{DataValue, ValueRef};

const BITS_MAX_INDEX: usize = 8;

pub type TupleId = usize;

#[derive(Clone, Debug, PartialEq)]
pub struct Tuple {
    pub id: Option<TupleId>,
    pub columns: Vec<ColumnRef>,
    pub values: Vec<ValueRef>,
}

impl Tuple {
    pub fn deserialize_from(id: Option<TupleId>, columns: Vec<ColumnRef>, bytes: &[u8]) -> Self {
        fn bit_index(bits: u8, i: usize) -> bool {
            bits & (1 << (7 - i)) > 0
        }

        let values_len = columns.len();
        let mut values = Vec::with_capacity(values_len);
        let bits_len = (values_len + BITS_MAX_INDEX) / BITS_MAX_INDEX;

        let mut pos = bits_len;

        for (i, col) in columns.iter().enumerate() {
            let logic_type = col.datatype();

            if bit_index(bytes[i / BITS_MAX_INDEX], i % BITS_MAX_INDEX) {
                values.push(Arc::new(DataValue::none(logic_type)));
            } else if let Some(len) = logic_type.raw_len() {
                values.push(Arc::new(DataValue::from_raw(&bytes[pos..pos + len], logic_type)));
                pos += len;
            } else {
                let len = u32::decode_fixed(&bytes[pos..pos + 4]) as usize;
                pos += 4;
                values.push(Arc::new(DataValue::from_raw(&bytes[pos..pos + len], logic_type)));
                pos += len;
            }
        }

        Tuple {
            id,
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
        let cells = tuple.values
            .iter()
            .map(|value| Cell::new(format!("{value}")))
            .collect_vec();

        table.add_row(cells);
    }

    table
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::types::LogicalType;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;

    #[test]
    fn test_tuple_serialize_to_and_deserialize_from() {
        let columns = vec![
            Arc::new(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, true)
            )),
            Arc::new(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::UInteger, false)
            )),
            Arc::new(ColumnCatalog::new(
                "c3".to_string(),
                false,
                ColumnDesc::new(LogicalType::Varchar, false)
            )),
            Arc::new(ColumnCatalog::new(
                "c4".to_string(),
                false,
                ColumnDesc::new(LogicalType::Smallint, false)
            )),
            Arc::new(ColumnCatalog::new(
                "c5".to_string(),
                false,
                ColumnDesc::new(LogicalType::USmallint, false)
            )),
            Arc::new(ColumnCatalog::new(
                "c6".to_string(),
                false,
                ColumnDesc::new(LogicalType::Float, false)
            )),
            Arc::new(ColumnCatalog::new(
                "c7".to_string(),
                false,
                ColumnDesc::new(LogicalType::Double, false)
            )),
            Arc::new(ColumnCatalog::new(
                "c8".to_string(),
                false,
                ColumnDesc::new(LogicalType::Tinyint, false)
            )),
            Arc::new(ColumnCatalog::new(
                "c9".to_string(),
                false,
                ColumnDesc::new(LogicalType::UTinyint, false)
            )),
            Arc::new(ColumnCatalog::new(
                "c10".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, false)
            )),
        ];

        let tuples = vec![
            Tuple {
                id: Some(0),
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
                ]
            },
            Tuple {
                id: Some(1),
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
                ],
            }
        ];

        let tuple_0 = Tuple::deserialize_from(
            Some(0),
            columns.clone(),
            &tuples[0].serialize_to()
        );
        let tuple_1 = Tuple::deserialize_from(
            Some(1),
            columns.clone(),
            &tuples[1].serialize_to()
        );

        assert_eq!(tuples[0], tuple_0);
        assert_eq!(tuples[1], tuple_1);
    }
}