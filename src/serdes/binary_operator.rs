use crate::expression::BinaryOperator;
use crate::serdes::Serialization;
use std::io;
use std::io::{Read, Write};

impl Serialization for BinaryOperator {
    type Error = io::Error;

    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        match self {
            BinaryOperator::Plus => writer.write_all(&[0u8])?,
            BinaryOperator::Minus => writer.write_all(&[1u8])?,
            BinaryOperator::Multiply => writer.write_all(&[2u8])?,
            BinaryOperator::Divide => writer.write_all(&[3u8])?,
            BinaryOperator::Modulo => writer.write_all(&[4u8])?,
            BinaryOperator::StringConcat => writer.write_all(&[5u8])?,
            BinaryOperator::Gt => writer.write_all(&[6u8])?,
            BinaryOperator::Lt => writer.write_all(&[7u8])?,
            BinaryOperator::GtEq => writer.write_all(&[8u8])?,
            BinaryOperator::LtEq => writer.write_all(&[9u8])?,
            BinaryOperator::Spaceship => writer.write_all(&[10u8])?,
            BinaryOperator::Eq => writer.write_all(&[11u8])?,
            BinaryOperator::NotEq => writer.write_all(&[12u8])?,
            BinaryOperator::Like(escape_char) => {
                writer.write_all(&[13u8])?;

                escape_char.encode(writer)?;
            }
            BinaryOperator::NotLike(escape_char) => {
                writer.write_all(&[14u8])?;

                escape_char.encode(writer)?;
            }
            BinaryOperator::And => writer.write_all(&[15u8])?,
            BinaryOperator::Or => writer.write_all(&[16u8])?,
            BinaryOperator::Xor => writer.write_all(&[17u8])?,
        }

        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut type_bytes = [0u8; 1];
        reader.read_exact(&mut type_bytes)?;

        Ok(match type_bytes[0] {
            0 => BinaryOperator::Plus,
            1 => BinaryOperator::Minus,
            2 => BinaryOperator::Multiply,
            3 => BinaryOperator::Divide,
            4 => BinaryOperator::Modulo,
            5 => BinaryOperator::StringConcat,
            6 => BinaryOperator::Gt,
            7 => BinaryOperator::Lt,
            8 => BinaryOperator::GtEq,
            9 => BinaryOperator::LtEq,
            10 => BinaryOperator::Spaceship,
            11 => BinaryOperator::Eq,
            12 => BinaryOperator::NotEq,
            13 => {
                let escape_char = Option::<char>::decode(reader)?;

                BinaryOperator::Like(escape_char)
            }
            14 => {
                let escape_char = Option::<char>::decode(reader)?;

                BinaryOperator::NotLike(escape_char)
            }
            15 => BinaryOperator::And,
            16 => BinaryOperator::Or,
            17 => BinaryOperator::Xor,
            _ => unreachable!(),
        })
    }
}
