use crate::serdes::Serialization;
use encode_unicode::CharExt;
use std::io;
use std::io::{Read, Write};

impl Serialization for char {
    type Error = io::Error;

    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        let (bytes, _) = self.to_utf8_array();

        writer.write_all(&bytes)
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;

        // SAFETY
        Ok(char::from_utf8_array(buf).unwrap())
    }
}
