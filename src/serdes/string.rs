use crate::serdes::Serialization;
use std::io;
use std::io::{Read, Write};

impl Serialization for String {
    type Error = io::Error;

    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        (self.len() as u32).encode(writer)?;
        writer.write_all(self.as_bytes())?;

        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut buf = vec![0u8; u32::decode(reader)? as usize];
        reader.read_exact(&mut buf)?;

        Ok(String::from_utf8(buf).unwrap())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::Serialization;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        let source = "hello".to_string();
        source.encode(&mut cursor)?;
        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(String::decode(&mut cursor)?, source);

        Ok(())
    }
}
