use crate::expression::UnaryOperator;
use crate::serdes::Serialization;
use std::io;
use std::io::{Read, Write};

impl Serialization for UnaryOperator {
    type Error = io::Error;

    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        let type_id = match self {
            UnaryOperator::Plus => 0u8,
            UnaryOperator::Minus => 1u8,
            UnaryOperator::Not => 2u8,
        };
        writer.write_all(&[type_id])?;

        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut type_bytes = [0u8; 1];
        reader.read_exact(&mut type_bytes)?;

        Ok(match type_bytes[0] {
            0 => UnaryOperator::Plus,
            1 => UnaryOperator::Minus,
            2 => UnaryOperator::Not,
            _ => unreachable!(),
        })
    }
}
