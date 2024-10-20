use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use std::io::{Read, Write};

impl<V> ReferenceSerialization for Option<V>
where
    V: ReferenceSerialization,
{
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        match self {
            None => 0u8.encode(writer, is_direct, reference_tables)?,
            Some(v) => {
                1u8.encode(writer, is_direct, reference_tables)?;
                v.encode(writer, is_direct, reference_tables)?;
            }
        }

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        match u8::decode(reader, drive, reference_tables)? {
            0 => Ok(None),
            1 => Ok(Some(V::decode(reader, drive, reference_tables)?)),
            _ => unreachable!(),
        }
    }
}
