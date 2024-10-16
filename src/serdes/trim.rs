use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use sqlparser::ast::TrimWhereField;
use std::io::{Read, Write};

impl ReferenceSerialization for TrimWhereField {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        _: bool,
        _: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        let type_id = match self {
            TrimWhereField::Both => 0,
            TrimWhereField::Leading => 1,
            TrimWhereField::Trailing => 2,
        };
        writer.write_all(&[type_id])?;

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        _: Option<(&T, &TableCache)>,
        _: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let mut one_byte = [0u8; 1];
        reader.read_exact(&mut one_byte)?;

        Ok(match one_byte[0] {
            0 => TrimWhereField::Both,
            1 => TrimWhereField::Leading,
            2 => TrimWhereField::Trailing,
            _ => unreachable!(),
        })
    }
}
