use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use std::io::{Read, Write};
use std::marker::PhantomData;

impl<V> ReferenceSerialization for PhantomData<V> {
    fn encode<W: Write>(
        &self,
        _: &mut W,
        _: bool,
        _: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        _: &mut R,
        _: Option<(&T, &TableCache)>,
        _: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        Ok(PhantomData)
    }
}
