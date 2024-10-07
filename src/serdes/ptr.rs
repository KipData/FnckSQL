use crate::serdes::DatabaseError;
use crate::serdes::TableCache;
use crate::serdes::Transaction;
use crate::serdes::{ReferenceSerialization, ReferenceTables, Serialization};
use std::io::{Read, Write};
use std::sync::Arc;

#[macro_export]
macro_rules! implement_ptr_serialization {
    ($struct_name:ident) => {
        impl<V> ReferenceSerialization for $struct_name<V>
        where
            V: ReferenceSerialization,
        {
            fn encode<W: Write>(
                &self,
                writer: &mut W,
                is_direct: bool,
                reference_tables: &mut ReferenceTables,
            ) -> Result<(), DatabaseError> {
                self.as_ref().encode(writer, is_direct, reference_tables)
            }

            fn decode<T: Transaction, R: Read>(
                reader: &mut R,
                drive: Option<(&T, &TableCache)>,
                reference_tables: &ReferenceTables,
            ) -> Result<Self, DatabaseError> {
                Ok($struct_name::from(V::decode(
                    reader,
                    drive,
                    reference_tables,
                )?))
            }
        }

        impl<V> Serialization for $struct_name<V>
        where
            V: Serialization,
        {
            type Error = V::Error;

            fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
                self.as_ref().encode(writer)
            }

            fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error> {
                Ok($struct_name::from(V::decode(reader)?))
            }
        }
    };
}

implement_ptr_serialization!(Arc);
implement_ptr_serialization!(Box);
