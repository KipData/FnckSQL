use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary};
use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use crate::types::ColumnId;
use std::io::{Read, Write};
use std::sync::Arc;

impl ReferenceSerialization for ColumnRef {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        self.summary.encode(writer, is_direct, reference_tables)?;
        if is_direct || matches!(self.summary.relation, ColumnRelation::None) {
            self.nullable.encode(writer, is_direct, reference_tables)?;
            self.desc.encode(writer, is_direct, reference_tables)?;
        }

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let summary = ColumnSummary::decode(reader, drive, reference_tables)?;

        if let (
            ColumnRelation::Table {
                column_id,
                table_name,
            },
            Some((transaction, table_cache)),
        ) = (&summary.relation, drive)
        {
            let table = transaction
                .table(table_cache, table_name.clone())
                .ok_or(DatabaseError::TableNotFound)?;
            let column = table
                .get_column_by_id(column_id)
                .ok_or(DatabaseError::InvalidColumn(format!(
                    "column id: {} not found",
                    column_id
                )))?;
            Ok(column.clone())
        } else {
            let nullable = bool::decode(reader, drive, reference_tables)?;
            let desc = ColumnDesc::decode(reader, drive, reference_tables)?;

            Ok(Self(Arc::new(ColumnCatalog {
                summary,
                nullable,
                desc,
            })))
        }
    }
}

impl ReferenceSerialization for ColumnRelation {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        match self {
            ColumnRelation::None => {
                writer.write_all(&[0])?;
            }
            ColumnRelation::Table {
                column_id,
                table_name,
            } => {
                writer.write_all(&[1])?;

                column_id.encode(writer, is_direct, reference_tables)?;

                reference_tables.push_or_replace(table_name).encode(
                    writer,
                    is_direct,
                    reference_tables,
                )?;
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
            0 => ColumnRelation::None,
            1 => {
                let column_id = ColumnId::decode(reader, drive, reference_tables)?;
                let table_name = reference_tables
                    .get(<usize as ReferenceSerialization>::decode(
                        reader,
                        drive,
                        reference_tables,
                    )?)
                    .clone();

                ColumnRelation::Table {
                    column_id,
                    table_name,
                }
            }
            _ => unreachable!(),
        })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary};
    use crate::db::test::build_table;
    use crate::errors::DatabaseError;
    use crate::expression::ScalarExpression;
    use crate::serdes::ReferenceSerialization;
    use crate::serdes::ReferenceTables;
    use crate::storage::rocksdb::{RocksStorage, RocksTransaction};
    use crate::storage::{StatisticsMetaCache, Storage, Transaction};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::ShardingLruCache;
    use std::hash::RandomState;
    use std::io::{Cursor, Seek, SeekFrom};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_column_serialization() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let table_cache = Arc::new(ShardingLruCache::new(4, 1, RandomState::new())?);
        let meta_cache = StatisticsMetaCache::new(4, 1, RandomState::new())?;

        let table_name = Arc::new("t1".to_string());
        build_table(&table_cache, &mut transaction)?;

        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();

        {
            let ref_column = ColumnRef(Arc::new(ColumnCatalog {
                summary: ColumnSummary {
                    name: "c3".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: 2,
                        table_name: table_name.clone(),
                    },
                },
                nullable: false,
                desc: ColumnDesc {
                    column_datatype: LogicalType::Integer,
                    is_primary: false,
                    is_unique: false,
                    default: None,
                },
            }));

            ref_column.encode(&mut cursor, false, &mut reference_tables)?;
            cursor.seek(SeekFrom::Start(0))?;

            assert_eq!(
                ColumnRef::decode::<RocksTransaction, Cursor<Vec<u8>>>(
                    &mut cursor,
                    Some((&transaction, &table_cache)),
                    &reference_tables
                )?,
                ref_column
            );
            cursor.seek(SeekFrom::Start(0))?;

            transaction.drop_column(&table_cache, &meta_cache, &table_name, "c3")?;
            assert!(ColumnRef::decode::<RocksTransaction, Cursor<Vec<u8>>>(
                &mut cursor,
                Some((&transaction, &table_cache)),
                &reference_tables
            )
            .is_err());
            cursor.seek(SeekFrom::Start(0))?;
        }
        {
            let not_ref_column = ColumnRef(Arc::new(ColumnCatalog {
                summary: ColumnSummary {
                    name: "c3".to_string(),
                    relation: ColumnRelation::None,
                },
                nullable: false,
                desc: ColumnDesc {
                    column_datatype: LogicalType::Integer,
                    is_primary: false,
                    is_unique: false,
                    default: Some(ScalarExpression::Constant(Arc::new(DataValue::UInt64(
                        Some(42),
                    )))),
                },
            }));
            not_ref_column.encode(&mut cursor, false, &mut reference_tables)?;
            cursor.seek(SeekFrom::Start(0))?;

            assert_eq!(
                ColumnRef::decode::<RocksTransaction, Cursor<Vec<u8>>>(
                    &mut cursor,
                    None,
                    &reference_tables
                )?,
                not_ref_column
            );
        }

        Ok(())
    }

    #[test]
    fn test_column_summary_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let summary = ColumnSummary {
            name: "c1".to_string(),
            relation: ColumnRelation::Table {
                column_id: 0,
                table_name: Arc::new("t1".to_string()),
            },
        };
        summary.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;

        assert_eq!(
            ColumnSummary::decode::<RocksTransaction, Cursor<Vec<u8>>>(
                &mut cursor,
                None,
                &reference_tables
            )?,
            summary
        );

        Ok(())
    }

    #[test]
    fn test_column_relation_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let none_relation = ColumnRelation::None;
        none_relation.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;

        let decode_relation = ColumnRelation::decode::<RocksTransaction, Cursor<Vec<u8>>>(
            &mut cursor,
            None,
            &reference_tables,
        )?;
        assert_eq!(none_relation, decode_relation);
        cursor.seek(SeekFrom::Start(0))?;
        let table_relation = ColumnRelation::Table {
            column_id: 0,
            table_name: Arc::new("t1".to_string()),
        };
        table_relation.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;

        let decode_relation = ColumnRelation::decode::<RocksTransaction, Cursor<Vec<u8>>>(
            &mut cursor,
            None,
            &reference_tables,
        )?;
        assert_eq!(table_relation, decode_relation);

        Ok(())
    }

    #[test]
    fn test_column_desc_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let desc = ColumnDesc {
            column_datatype: LogicalType::Integer,
            is_primary: false,
            is_unique: false,
            default: Some(ScalarExpression::Constant(Arc::new(DataValue::UInt64(
                Some(42),
            )))),
        };
        desc.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;

        let decode_desc = ColumnDesc::decode::<RocksTransaction, Cursor<Vec<u8>>>(
            &mut cursor,
            None,
            &reference_tables,
        )?;
        assert_eq!(desc, decode_desc);

        Ok(())
    }
}
