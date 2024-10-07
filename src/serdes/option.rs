use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables, Serialization};
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
            None => 0u8.encode(writer)?,
            Some(v) => {
                1u8.encode(writer)?;
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
        match u8::decode(reader)? {
            0 => Ok(None),
            1 => Ok(Some(V::decode(reader, drive, reference_tables)?)),
            _ => unreachable!(),
        }
    }
}

impl<V> Serialization for Option<V>
where
    V: Serialization,
{
    type Error = V::Error;

    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        match self {
            None => 0u8.encode(writer)?,
            Some(v) => {
                1u8.encode(writer)?;
                v.encode(writer)?;
            }
        }

        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error> {
        match u8::decode(reader)? {
            0 => Ok(None),
            1 => Ok(Some(V::decode(reader)?)),
            _ => unreachable!(),
        }
    }
}
