use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use crate::types::value::{DataValue, ValueRef};
use crate::types::LogicalType;
use std::io::{Read, Write};

impl ReferenceSerialization for DataValue {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        let logical_type = self.logical_type();

        logical_type.encode(writer, is_direct, reference_tables)?;
        self.is_null().encode(writer, is_direct, reference_tables)?;

        if self.is_null() {
            return Ok(());
        }
        if let DataValue::Tuple(values) = self {
            values.encode(writer, is_direct, reference_tables)?;
            return Ok(());
        }
        if logical_type.raw_len().is_none() {
            let mut bytes = Vec::new();
            self.to_raw(&mut bytes)?
                .encode(writer, is_direct, reference_tables)?;
            writer.write_all(&bytes)?;
        } else {
            let _ = self.to_raw(writer)?;
        }

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let logical_type = LogicalType::decode(reader, drive, reference_tables)?;

        if bool::decode(reader, drive, reference_tables)? {
            return Ok(DataValue::none(&logical_type));
        }
        if matches!(logical_type, LogicalType::Tuple) {
            return Ok(DataValue::Tuple(Option::<Vec<ValueRef>>::decode(
                reader,
                drive,
                reference_tables,
            )?));
        }
        let value_len = match logical_type.raw_len() {
            None => usize::decode(reader, drive, reference_tables)?,
            Some(len) => len,
        };
        let mut buf = vec![0u8; value_len];
        reader.read_exact(&mut buf)?;

        Ok(DataValue::from_raw(&buf, &logical_type))
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
    use std::sync::Arc;

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
        let source_5 = DataValue::Tuple(Some(vec![
            Arc::new(DataValue::Int32(None)),
            Arc::new(DataValue::Int32(Some(42))),
        ]));

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
