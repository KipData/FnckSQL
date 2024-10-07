use crate::serdes::Serialization;
use std::io;
use std::io::Read;
use std::io::Write;
use std::mem::size_of;

#[macro_export]
macro_rules! implement_num_serialization {
    ($struct_name:ident) => {
        impl Serialization for $struct_name {
            type Error = io::Error;

            fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
                writer.write_all(&self.to_le_bytes()[..])
            }

            fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error> {
                let mut bytes = [0u8; size_of::<Self>()];
                reader.read_exact(&mut bytes)?;

                Ok(Self::from_le_bytes(bytes))
            }
        }
    };
}

implement_num_serialization!(i8);
implement_num_serialization!(i16);
implement_num_serialization!(i32);
implement_num_serialization!(i64);
implement_num_serialization!(u8);
implement_num_serialization!(u16);
implement_num_serialization!(u32);
implement_num_serialization!(u64);

#[cfg(test)]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::Serialization;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        let source_0 = 8u8;
        let source_1 = 16u16;
        let source_2 = 32u32;
        let source_3 = 64u64;
        let source_4 = 8i8;
        let source_5 = 16i16;
        let source_6 = 32i32;
        let source_7 = 64i64;

        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source_0.encode(&mut cursor)?;
        source_1.encode(&mut cursor)?;
        source_2.encode(&mut cursor)?;
        source_3.encode(&mut cursor)?;
        source_4.encode(&mut cursor)?;
        source_5.encode(&mut cursor)?;
        source_6.encode(&mut cursor)?;
        source_7.encode(&mut cursor)?;

        cursor.seek(SeekFrom::Start(0))?;

        let decoded_0 = u8::decode(&mut cursor).unwrap();
        let decoded_1 = u16::decode(&mut cursor).unwrap();
        let decoded_2 = u32::decode(&mut cursor).unwrap();
        let decoded_3 = u64::decode(&mut cursor).unwrap();
        let decoded_4 = i8::decode(&mut cursor).unwrap();
        let decoded_5 = i16::decode(&mut cursor).unwrap();
        let decoded_6 = i32::decode(&mut cursor).unwrap();
        let decoded_7 = i64::decode(&mut cursor).unwrap();

        assert_eq!(source_0, decoded_0);
        assert_eq!(source_1, decoded_1);
        assert_eq!(source_2, decoded_2);
        assert_eq!(source_3, decoded_3);
        assert_eq!(source_4, decoded_4);
        assert_eq!(source_5, decoded_5);
        assert_eq!(source_6, decoded_6);
        assert_eq!(source_7, decoded_7);

        Ok(())
    }
}
