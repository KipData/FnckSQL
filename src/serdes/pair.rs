use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use std::io::{Read, Write};

impl<A, B> ReferenceSerialization for (A, B)
where
    A: ReferenceSerialization,
    B: ReferenceSerialization,
{
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        let (v1, v2) = self;
        v1.encode(writer, is_direct, reference_tables)?;
        v2.encode(writer, is_direct, reference_tables)?;

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let v1 = A::decode(reader, drive, reference_tables)?;
        let v2 = B::decode(reader, drive, reference_tables)?;

        Ok((v1, v2))
    }
}
