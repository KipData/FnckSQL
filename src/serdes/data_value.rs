use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use std::io::{Read, Write};

impl DataValue {
    // FIXME: redundant code
    pub(crate) fn inner_encode<W: Write>(
        &self,
        writer: &mut W,
        ty: &LogicalType,
    ) -> Result<(), DatabaseError> {
        writer.write_all(&[if self.is_null() { 0u8 } else { 1u8 }])?;

        if self.is_null() {
            return Ok(());
        }
        if let DataValue::Tuple(values) = self {
            match values {
                None => writer.write_all(&[0u8])?,
                Some((values, is_upper)) => {
                    writer.write_all(&[1u8])?;
                    writer.write_all(&(values.len() as u32).to_le_bytes())?;
                    for value in values.iter() {
                        value.inner_encode(writer, &value.logical_type())?
                    }
                    writer.write_all(&[if *is_upper { 1u8 } else { 0u8 }])?;
                }
            }

            return Ok(());
        }
        if ty.raw_len().is_none() {
            let mut bytes = Vec::new();
            writer.write_all(&(self.to_raw(&mut bytes)? as u32).to_le_bytes())?;
            writer.write_all(&bytes)?;
        } else {
            let _ = self.to_raw(writer)?;
        }

        Ok(())
    }

    pub(crate) fn inner_decode<R: Read>(
        reader: &mut R,
        ty: &LogicalType,
    ) -> Result<Self, DatabaseError> {
        let mut bytes = [0u8; 1];
        reader.read_exact(&mut bytes)?;
        if bytes[0] == 0 {
            return Ok(DataValue::none(ty));
        }
        if let LogicalType::Tuple(types) = ty {
            let mut bytes = [0u8; 1];
            reader.read_exact(&mut bytes)?;
            let values = match bytes[0] {
                0 => None,
                1 => {
                    let mut bytes = [0u8; 4];
                    reader.read_exact(&mut bytes)?;
                    let len = u32::from_le_bytes(bytes) as usize;
                    let mut vec = Vec::with_capacity(len);

                    for ty in types.iter() {
                        vec.push(Self::inner_decode(reader, ty)?);
                    }
                    let mut bytes = [0u8];
                    reader.read_exact(&mut bytes)?;
                    Some((vec, bytes[0] == 1))
                }
                _ => unreachable!(),
            };

            return Ok(DataValue::Tuple(values));
        }
        let value_len = match ty.raw_len() {
            None => {
                let mut bytes = [0u8; 4];
                reader.read_exact(&mut bytes)?;
                u32::from_le_bytes(bytes) as usize
            }
            Some(len) => len,
        };
        let mut buf = vec![0u8; value_len];
        reader.read_exact(&mut buf)?;

        Ok(DataValue::from_raw(&buf, ty))
    }
}

impl ReferenceSerialization for DataValue {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        let ty = self.logical_type();
        ty.encode(writer, is_direct, reference_tables)?;

        self.inner_encode(writer, &ty)
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let logical_type = LogicalType::decode(reader, drive, reference_tables)?;

        Self::inner_decode(reader, &logical_type)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::value::{DataValue, Utf8Type};
    use sqlparser::ast::CharLengthUnits;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        let source_0 = DataValue::Int32(None);
        let source_1 = DataValue::Int32(Some(32));
        let source_2 = DataValue::Utf8 {
            value: None,
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        };
        let source_3 = DataValue::Utf8 {
            value: Some("hello".to_string()),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        };
        let source_4 = DataValue::Tuple(None);
        let source_5 = DataValue::Tuple(Some((
            vec![DataValue::Int32(None), DataValue::Int32(Some(42))],
            false,
        )));

        let mut reference_tables = ReferenceTables::new();
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source_0.encode(&mut cursor, false, &mut reference_tables)?;
        source_1.encode(&mut cursor, false, &mut reference_tables)?;
        source_2.encode(&mut cursor, false, &mut reference_tables)?;
        source_3.encode(&mut cursor, false, &mut reference_tables)?;
        source_4.encode(&mut cursor, false, &mut reference_tables)?;
        source_5.encode(&mut cursor, false, &mut reference_tables)?;

        cursor.seek(SeekFrom::Start(0))?;

        let decoded_0 =
            DataValue::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_1 =
            DataValue::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_2 =
            DataValue::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_3 =
            DataValue::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_4 =
            DataValue::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_5 =
            DataValue::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();

        assert_eq!(source_0, decoded_0);
        assert_eq!(source_1, decoded_1);
        assert_eq!(source_2, decoded_2);
        assert_eq!(source_3, decoded_3);
        assert_eq!(source_4, decoded_4);
        assert_eq!(source_5, decoded_5);

        Ok(())
    }
}
