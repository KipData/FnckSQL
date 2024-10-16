use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use std::io::{Read, Write};

impl<V> ReferenceSerialization for [V; 2]
where
    V: ReferenceSerialization,
{
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        self[0].encode(writer, is_direct, reference_tables)?;
        self[1].encode(writer, is_direct, reference_tables)?;

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        Ok([
            V::decode(reader, drive, reference_tables)?,
            V::decode(reader, drive, reference_tables)?,
        ])
    }
}
