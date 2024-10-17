use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use std::io::{Read, Write};
use ulid::Ulid;

impl ReferenceSerialization for Ulid {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        _: bool,
        _: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        writer.write_all(&self.to_bytes()[..])?;

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        _: Option<(&T, &TableCache)>,
        _: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let mut buf = [0u8; 16];
        reader.read_exact(&mut buf)?;

        Ok(Ulid::from_bytes(buf))
    }
}
