use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use encode_unicode::CharExt;
use std::io::{Read, Write};

impl ReferenceSerialization for char {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        _: bool,
        _: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        let (bytes, _) = self.to_utf8_array();

        Ok(writer.write_all(&bytes)?)
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        _: Option<(&T, &TableCache)>,
        _: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;

        // SAFETY
        Ok(char::from_utf8_array(buf).unwrap())
    }
}
