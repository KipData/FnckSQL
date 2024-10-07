mod alias_type;
mod binary_operator;
mod boolean;
mod char;
mod char_length_units;
mod column;
mod evaluator;
mod logic_type;
mod num;
mod option;
mod ptr;
mod scala_expression;
mod scala_function;
mod string;
mod table_function;
mod trim;
mod unary_operator;

use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::storage::{TableCache, Transaction};
use std::io::{Read, Write};
use std::{io, mem};

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

pub trait Serialization: Sized {
    type Error: From<io::Error> + std::error::Error + Send + Sync + 'static;

    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error>;

    fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error>;
}

pub enum ReferenceTables {
    Single(Option<TableName>),
    Multiple(Vec<TableName>),
}

impl ReferenceTables {
    pub fn single() -> Self {
        ReferenceTables::Single(None)
    }

    pub fn multiple() -> Self {
        ReferenceTables::Multiple(Vec::new())
    }

    pub fn get(&self, i: usize) -> &TableName {
        match self {
            ReferenceTables::Single(table_name) => table_name.as_ref().unwrap(),
            ReferenceTables::Multiple(tables) => &tables[i],
        }
    }

    pub fn push_or_replace(&mut self, table_name: &TableName) -> usize {
        match self {
            ReferenceTables::Single(table) => {
                let _ = mem::replace(table, Some(table_name.clone()));
                0
            }
            ReferenceTables::Multiple(tables) => {
                for (i, item) in tables.iter().enumerate() {
                    if item == table_name {
                        return i;
                    }
                }
                tables.push(table_name.clone());
                tables.len() - 1
            }
        }
    }
}
