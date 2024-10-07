use crate::errors::DatabaseError;
use crate::serdes::Serialization;
use sqlparser::ast::CharLengthUnits;
use std::io::{Read, Write};

impl Serialization for CharLengthUnits {
    type Error = DatabaseError;

    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        match self {
            CharLengthUnits::Characters => 0u8,
            CharLengthUnits::Octets => 1u8,
        }
        .encode(writer)?;

        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut one_byte = [0u8; 1];
        reader.read_exact(&mut one_byte)?;

        Ok(match one_byte[0] {
            0 => CharLengthUnits::Characters,
            1 => CharLengthUnits::Octets,
            _ => unreachable!(),
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::Serialization;
    use sqlparser::ast::CharLengthUnits;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());

        CharLengthUnits::Characters.encode(&mut cursor)?;
        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(
            CharLengthUnits::decode(&mut cursor)?,
            CharLengthUnits::Characters
        );
        cursor.seek(SeekFrom::Start(0))?;
        CharLengthUnits::Octets.encode(&mut cursor)?;
        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(
            CharLengthUnits::decode(&mut cursor)?,
            CharLengthUnits::Octets
        );

        Ok(())
    }
}
