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
use std::io;
use std::io::{Read, Write};

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
