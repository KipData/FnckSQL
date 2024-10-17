mod boolean;
mod bound;
mod char;
mod char_length_units;
mod column;
mod data_value;
mod evaluator;
mod function;
mod hasher;
mod num;
mod option;
mod pair;
mod path_buf;
mod phantom;
mod ptr;
mod slice;
mod string;
mod trim;
mod ulid;
mod vec;

use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::storage::{TableCache, Transaction};
use std::io::{Read, Write};

#[macro_export]
macro_rules! implement_serialization_by_bincode {
    ($struct_name:ident) => {
        impl $crate::serdes::ReferenceSerialization for $struct_name {
            fn encode<W: std::io::Write>(
                &self,
                writer: &mut W,
                is_direct: bool,
                reference_tables: &mut $crate::serdes::ReferenceTables,
            ) -> Result<(), $crate::errors::DatabaseError> {
                let bytes = bincode::serialize(self)?;
                $crate::serdes::ReferenceSerialization::encode(
                    &bytes.len(),
                    writer,
                    is_direct,
                    reference_tables,
                )?;
                std::io::Write::write_all(writer, &bytes)?;

                Ok(())
            }

            fn decode<T: $crate::storage::Transaction, R: std::io::Read>(
                reader: &mut R,
                drive: Option<(&T, &$crate::storage::TableCache)>,
                reference_tables: &$crate::serdes::ReferenceTables,
            ) -> Result<Self, $crate::errors::DatabaseError> {
                let mut buf = vec![
                    0u8;
                    <usize as $crate::serdes::ReferenceSerialization>::decode(
                        reader,
                        drive,
                        reference_tables
                    )?
                ];
                std::io::Read::read_exact(reader, &mut buf)?;

                Ok(bincode::deserialize::<Self>(&buf)?)
            }
        }
    };
}

pub trait ReferenceSerialization: Sized {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError>;

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError>;
}

#[derive(Default)]
pub struct ReferenceTables {
    tables: Vec<TableName>,
}

impl ReferenceTables {
    pub fn new() -> Self {
        ReferenceTables { tables: vec![] }
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    pub fn len(&self) -> usize {
        self.tables.len()
    }

    pub fn get(&self, i: usize) -> &TableName {
        &self.tables[i]
    }

    pub fn push_or_replace(&mut self, table_name: &TableName) -> usize {
        for (i, item) in self.tables.iter().enumerate() {
            if item == table_name {
                return i;
            }
        }
        self.tables.push(table_name.clone());
        self.tables.len() - 1
    }
}
