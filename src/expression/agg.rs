use crate::serdes::Serialization;
use serde::{Deserialize, Serialize};
use std::io;
use std::io::{Read, Write};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggKind {
    Avg,
    Max,
    Min,
    Sum,
    Count,
}

impl AggKind {
    pub fn allow_distinct(&self) -> bool {
        match self {
            AggKind::Avg => false,
            AggKind::Max => false,
            AggKind::Min => false,
            AggKind::Sum => true,
            AggKind::Count => true,
        }
    }
}

impl Serialization for AggKind {
    type Error = io::Error;

    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        let type_id = match self {
            AggKind::Avg => 0u8,
            AggKind::Max => 1u8,
            AggKind::Min => 2u8,
            AggKind::Sum => 3u8,
            AggKind::Count => 4u8,
        };
        writer.write_all(&[type_id])
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut type_bytes = [0u8; 1];
        reader.read_exact(&mut type_bytes)?;

        Ok(match type_bytes[0] {
            0 => AggKind::Avg,
            1 => AggKind::Max,
            2 => AggKind::Min,
            3 => AggKind::Sum,
            4 => AggKind::Count,
            _ => unreachable!(),
        })
    }
}
