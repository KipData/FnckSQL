use crate::implement_serialization_by_bincode;

implement_serialization_by_bincode!(String);

#[cfg(test)]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);
        let mut reference_tables = ReferenceTables::new();

        let source = "hello".to_string();
        ReferenceSerialization::encode(&source, &mut cursor, true, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(
            String::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables)?,
            source
        );

        Ok(())
    }
}
