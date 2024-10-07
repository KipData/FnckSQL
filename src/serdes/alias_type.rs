use crate::errors::DatabaseError;
use crate::expression::{AliasType, ScalarExpression};
use crate::serdes::{ReferenceSerialization, ReferenceTables, Serialization};
use crate::storage::{TableCache, Transaction};
use std::io::{Read, Write};

impl ReferenceSerialization for AliasType {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        match self {
            AliasType::Name(name) => {
                writer.write_all(&[0u8])?;

                name.encode(writer)?;
            }
            AliasType::Expr(expr) => {
                writer.write_all(&[1u8])?;

                expr.encode(writer, is_direct, reference_tables)?;
            }
        }

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let mut type_bytes = [0u8; 1];
        reader.read_exact(&mut type_bytes)?;

        Ok(match type_bytes[0] {
            0 => AliasType::Name(String::decode(reader)?),
            1 => AliasType::Expr(Box::new(ScalarExpression::decode(
                reader,
                drive,
                reference_tables,
            )?)),
            _ => unreachable!(),
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::expression::AliasType;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);
        let mut reference_tables = ReferenceTables::single();

        let source = AliasType::Name("hello".to_string());
        source.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(
            AliasType::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables)?,
            source
        );

        Ok(())
    }
}
