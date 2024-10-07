use crate::serdes::Serialization;
use std::io;
use std::io::{Read, Write};

impl Serialization for bool {
    type Error = io::Error;

    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        if *self { 1u8 } else { 0u8 }.encode(writer)
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error> {
        Ok(u8::decode(reader)? == 1u8)
    }
}
