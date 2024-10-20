use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use crate::types::value::DataValue;
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
            Ok(DataValue::none(&logical_type))
        } else {
            let value_len = match logical_type.raw_len() {
                None => usize::decode(reader, drive, reference_tables)?,
                Some(len) => len,
            };
            let mut buf = vec![0u8; value_len];
            reader.read_exact(&mut buf)?;

            Ok(DataValue::from_raw(&buf, &logical_type))
        }
    }
}
