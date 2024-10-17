use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use std::io::{Read, Write};

impl<V> ReferenceSerialization for Vec<V>
where
    V: ReferenceSerialization,
{
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        self.len().encode(writer, is_direct, reference_tables)?;
        for value in self.iter() {
            value.encode(writer, is_direct, reference_tables)?
        }
        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let len = <usize as ReferenceSerialization>::decode(reader, drive, reference_tables)?;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(V::decode(reader, drive, reference_tables)?);
        }
        Ok(vec)
    }
}
