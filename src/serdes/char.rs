use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use std::io::{Read, Write};

impl ReferenceSerialization for char {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        _: bool,
        _: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        let mut buf = [0u8; 2];
        self.encode_utf8(&mut buf);

        Ok(writer.write_all(&buf)?)
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        _: Option<(&T, &TableCache)>,
        _: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf)?;

        // SAFETY
        Ok(std::str::from_utf8(&buf)?.chars().next().unwrap())
    }
}
