use crate::errors::DatabaseError;
use crate::serdes::Serialization;
use crate::types::LogicalType;
use sqlparser::ast::CharLengthUnits;
use std::io::{Read, Write};

impl Serialization for LogicalType {
    type Error = DatabaseError;

    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), Self::Error> {
        match self {
            LogicalType::Invalid => writer.write_all(&[0u8])?,
            LogicalType::SqlNull => writer.write_all(&[1u8])?,
            LogicalType::Boolean => writer.write_all(&[2u8])?,
            LogicalType::Tinyint => writer.write_all(&[3u8])?,
            LogicalType::UTinyint => writer.write_all(&[4u8])?,
            LogicalType::Smallint => writer.write_all(&[5u8])?,
            LogicalType::USmallint => writer.write_all(&[6u8])?,
            LogicalType::Integer => writer.write_all(&[7u8])?,
            LogicalType::UInteger => writer.write_all(&[8u8])?,
            LogicalType::Bigint => writer.write_all(&[9u8])?,
            LogicalType::UBigint => writer.write_all(&[10u8])?,
            LogicalType::Float => writer.write_all(&[11u8])?,
            LogicalType::Double => writer.write_all(&[12u8])?,
            LogicalType::Char(len, units) => {
                writer.write_all(&[13u8])?;
                len.encode(writer)?;
                units.encode(writer)?;
            }
            LogicalType::Varchar(len, units) => {
                writer.write_all(&[14u8])?;

                len.encode(writer)?;
                units.encode(writer)?;
            }
            LogicalType::Date => writer.write_all(&[15u8])?,
            LogicalType::DateTime => writer.write_all(&[16u8])?,
            LogicalType::Time => writer.write_all(&[17u8])?,
            LogicalType::Decimal(precision, scala) => {
                writer.write_all(&[18u8])?;

                precision.encode(writer)?;
                scala.encode(writer)?;
            }
            LogicalType::Tuple => writer.write_all(&[19u8])?,
        }

        Ok(())
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, Self::Error> {
        let mut one_byte = [0u8; 1];
        reader.read_exact(&mut one_byte)?;

        Ok(match one_byte[0] {
            0 => LogicalType::Invalid,
            1 => LogicalType::SqlNull,
            2 => LogicalType::Boolean,
            3 => LogicalType::Tinyint,
            4 => LogicalType::UTinyint,
            5 => LogicalType::Smallint,
            6 => LogicalType::USmallint,
            7 => LogicalType::Integer,
            8 => LogicalType::UInteger,
            9 => LogicalType::Bigint,
            10 => LogicalType::UBigint,
            11 => LogicalType::Float,
            12 => LogicalType::Double,
            13 => {
                let len = u32::decode(reader)?;
                let units = CharLengthUnits::decode(reader)?;

                LogicalType::Char(len, units)
            }
            14 => {
                let len = Option::<u32>::decode(reader)?;
                let units = CharLengthUnits::decode(reader)?;

                LogicalType::Varchar(len, units)
            }
            15 => LogicalType::Date,
            16 => LogicalType::DateTime,
            17 => LogicalType::Time,
            18 => {
                let precision = Option::<u8>::decode(reader)?;
                let scala = Option::<u8>::decode(reader)?;

                LogicalType::Decimal(precision, scala)
            }
            19 => LogicalType::Tuple,
            _ => unreachable!(),
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::{ReferenceTables, Serialization};
    use crate::types::LogicalType;
    use sqlparser::ast::CharLengthUnits;
    use std::io::{Cursor, Read, Seek, SeekFrom, Write};
    use std::sync::Arc;

    #[test]
    fn test_logic_type_serialization() -> Result<(), DatabaseError> {
        fn fn_assert(
            cursor: &mut Cursor<Vec<u8>>,
            logical_type: LogicalType,
        ) -> Result<(), DatabaseError> {
            logical_type.encode(cursor)?;

            cursor.seek(SeekFrom::Start(0))?;
            assert_eq!(LogicalType::decode(cursor)?, logical_type);
            cursor.seek(SeekFrom::Start(0))?;

            Ok(())
        }

        let mut cursor = Cursor::new(Vec::new());

        fn_assert(&mut cursor, LogicalType::Invalid)?;
        fn_assert(&mut cursor, LogicalType::SqlNull)?;
        fn_assert(&mut cursor, LogicalType::Boolean)?;
        fn_assert(&mut cursor, LogicalType::Tinyint)?;
        fn_assert(&mut cursor, LogicalType::UTinyint)?;
        fn_assert(&mut cursor, LogicalType::Smallint)?;
        fn_assert(&mut cursor, LogicalType::USmallint)?;
        fn_assert(&mut cursor, LogicalType::Integer)?;
        fn_assert(&mut cursor, LogicalType::UInteger)?;
        fn_assert(&mut cursor, LogicalType::Bigint)?;
        fn_assert(&mut cursor, LogicalType::UBigint)?;
        fn_assert(&mut cursor, LogicalType::Float)?;
        fn_assert(&mut cursor, LogicalType::Double)?;
        fn_assert(
            &mut cursor,
            LogicalType::Char(42, CharLengthUnits::Characters),
        )?;
        fn_assert(&mut cursor, LogicalType::Char(42, CharLengthUnits::Octets))?;
        fn_assert(
            &mut cursor,
            LogicalType::Varchar(Some(42), CharLengthUnits::Characters),
        )?;
        fn_assert(
            &mut cursor,
            LogicalType::Varchar(None, CharLengthUnits::Characters),
        )?;
        fn_assert(
            &mut cursor,
            LogicalType::Varchar(Some(42), CharLengthUnits::Octets),
        )?;
        fn_assert(
            &mut cursor,
            LogicalType::Varchar(None, CharLengthUnits::Octets),
        )?;
        fn_assert(&mut cursor, LogicalType::Date)?;
        fn_assert(&mut cursor, LogicalType::DateTime)?;
        fn_assert(&mut cursor, LogicalType::Time)?;
        fn_assert(&mut cursor, LogicalType::Decimal(Some(4), Some(2)))?;
        fn_assert(&mut cursor, LogicalType::Decimal(Some(4), None))?;
        fn_assert(&mut cursor, LogicalType::Decimal(None, Some(2)))?;
        fn_assert(&mut cursor, LogicalType::Decimal(None, None))?;
        fn_assert(&mut cursor, LogicalType::Tuple)?;

        Ok(())
    }
}
