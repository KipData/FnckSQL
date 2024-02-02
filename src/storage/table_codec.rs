use crate::catalog::{ColumnCatalog, ColumnRef, TableMeta};
use crate::errors::DatabaseError;
use crate::types::index::{Index, IndexId, IndexMeta};
use crate::types::tuple::{Tuple, TupleId};
use crate::types::LogicalType;
use bytes::Bytes;
use lazy_static::lazy_static;
use std::sync::Arc;

const BOUND_MIN_TAG: u8 = 0;
const BOUND_MAX_TAG: u8 = 1;

lazy_static! {
    static ref ROOT_BYTES: Vec<u8> = b"Root".to_vec();
}

#[derive(Clone)]
pub struct TableCodec {}

#[derive(Copy, Clone)]
enum CodecType {
    Column,
    IndexMeta,
    Index,
    Tuple,
    Root,
}

impl TableCodec {
    /// TableName + Type
    ///
    /// Tips: Root full key = key_prefix
    fn key_prefix(ty: CodecType, table_name: &str) -> Vec<u8> {
        let mut table_bytes = table_name.to_string().into_bytes();

        match ty {
            CodecType::Column => {
                table_bytes.push(b'0');
            }
            CodecType::IndexMeta => {
                table_bytes.push(b'1');
            }
            CodecType::Index => {
                table_bytes.push(b'3');
            }
            CodecType::Tuple => {
                table_bytes.push(b'8');
            }
            CodecType::Root => {
                let mut bytes = ROOT_BYTES.clone();
                bytes.push(BOUND_MIN_TAG);
                bytes.append(&mut table_bytes);

                table_bytes = bytes
            }
        }

        table_bytes
    }

    pub fn tuple_bound(table_name: &str) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            let mut key_prefix = Self::key_prefix(CodecType::Tuple, table_name);

            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn index_meta_bound(table_name: &str) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            let mut key_prefix = Self::key_prefix(CodecType::IndexMeta, table_name);

            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn index_bound(table_name: &str, index_id: &IndexId) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            let mut key_prefix = Self::key_prefix(CodecType::Index, table_name);

            key_prefix.push(BOUND_MIN_TAG);
            key_prefix.append(&mut index_id.to_be_bytes().to_vec());
            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn all_index_bound(table_name: &str) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            let mut key_prefix = Self::key_prefix(CodecType::Index, table_name);

            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn root_table_bound() -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            let mut key_prefix = ROOT_BYTES.clone();

            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn table_bound(table_name: &str) -> (Vec<u8>, Vec<u8>) {
        let mut column_prefix = Self::key_prefix(CodecType::Column, table_name);
        column_prefix.push(BOUND_MIN_TAG);

        let mut index_prefix = Self::key_prefix(CodecType::IndexMeta, table_name);
        index_prefix.push(BOUND_MAX_TAG);

        (column_prefix, index_prefix)
    }

    pub fn columns_bound(table_name: &str) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            let mut key_prefix = Self::key_prefix(CodecType::Column, table_name);

            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    /// Key: {TableName}{TUPLE_TAG}{BOUND_MIN_TAG}{RowID}(Sorted)
    /// Value: Tuple
    pub fn encode_tuple(table_name: &str, tuple: &Tuple) -> Result<(Bytes, Bytes), DatabaseError> {
        let tuple_id = tuple.id.clone().ok_or(DatabaseError::PrimaryKeyNotFound)?;
        let key = Self::encode_tuple_key(table_name, &tuple_id)?;

        Ok((Bytes::from(key), Bytes::from(tuple.serialize_to())))
    }

    pub fn encode_tuple_key(
        table_name: &str,
        tuple_id: &TupleId,
    ) -> Result<Vec<u8>, DatabaseError> {
        let mut key_prefix = Self::key_prefix(CodecType::Tuple, table_name);
        key_prefix.push(BOUND_MIN_TAG);

        if !matches!(
            tuple_id.logical_type(),
            LogicalType::Tinyint
                | LogicalType::Smallint
                | LogicalType::Integer
                | LogicalType::Bigint
                | LogicalType::UTinyint
                | LogicalType::USmallint
                | LogicalType::UInteger
                | LogicalType::UBigint
                | LogicalType::Varchar(_)
        ) {
            return Err(DatabaseError::InvalidType);
        }
        tuple_id.memcomparable_encode(&mut key_prefix)?;

        Ok(key_prefix)
    }

    pub fn decode_tuple(
        table_types: &[LogicalType],
        projections: &[usize],
        tuple_columns: &Arc<Vec<ColumnRef>>,
        bytes: &[u8],
    ) -> Tuple {
        Tuple::deserialize_from(table_types, projections, tuple_columns, bytes)
    }

    /// Key: {TableName}{INDEX_META_TAG}{BOUND_MIN_TAG}{IndexID}
    /// Value: IndexMeta
    pub fn encode_index_meta(
        table_name: &str,
        index_meta: &IndexMeta,
    ) -> Result<(Bytes, Bytes), DatabaseError> {
        let mut key_prefix = Self::key_prefix(CodecType::IndexMeta, table_name);
        key_prefix.push(BOUND_MIN_TAG);
        key_prefix.append(&mut index_meta.id.to_be_bytes().to_vec());

        Ok((
            Bytes::from(key_prefix),
            Bytes::from(bincode::serialize(&index_meta)?),
        ))
    }

    pub fn decode_index_meta(bytes: &[u8]) -> Result<IndexMeta, DatabaseError> {
        Ok(bincode::deserialize(bytes)?)
    }

    /// NonUnique Index:
    /// Key: {TableName}{INDEX_TAG}{BOUND_MIN_TAG}{IndexID}{BOUND_MIN_TAG}{DataValue1}{DataValue2} ..
    /// Value: TupleIDs
    ///
    /// Unique Index:
    /// Key: {TableName}{INDEX_TAG}{BOUND_MIN_TAG}{IndexID}{BOUND_MIN_TAG}{DataValue}
    /// Value: TupleIDs
    ///
    /// Tips: The unique index has only one ColumnID and one corresponding DataValue,
    /// so it can be positioned directly.
    pub fn encode_index(
        name: &str,
        index: &Index,
        tuple_ids: &[TupleId],
    ) -> Result<(Bytes, Bytes), DatabaseError> {
        let key = TableCodec::encode_index_key(name, index)?;

        Ok((
            Bytes::from(key),
            Bytes::from(bincode::serialize(tuple_ids)?),
        ))
    }

    pub fn encode_index_key(name: &str, index: &Index) -> Result<Vec<u8>, DatabaseError> {
        let mut key_prefix = Self::key_prefix(CodecType::Index, name);
        key_prefix.push(BOUND_MIN_TAG);
        key_prefix.append(&mut index.id.to_be_bytes().to_vec());
        key_prefix.push(BOUND_MIN_TAG);

        for col_v in &index.column_values {
            col_v.memcomparable_encode(&mut key_prefix)?;
            key_prefix.push(BOUND_MIN_TAG);
        }

        Ok(key_prefix)
    }

    pub fn decode_index(bytes: &[u8]) -> Result<Vec<TupleId>, DatabaseError> {
        Ok(bincode::deserialize(bytes)?)
    }

    /// Key: {TableName}{COLUMN_TAG}{BOUND_MIN_TAG}{ColumnId}
    /// Value: ColumnCatalog
    ///
    /// Tips: the `0` for bound range
    pub fn encode_column(
        table_name: &str,
        col: &ColumnCatalog,
    ) -> Result<(Bytes, Bytes), DatabaseError> {
        let bytes = bincode::serialize(col)?;
        let mut key_prefix = Self::key_prefix(CodecType::Column, table_name);

        key_prefix.push(BOUND_MIN_TAG);
        key_prefix.append(&mut col.id().unwrap().to_be_bytes().to_vec());

        Ok((Bytes::from(key_prefix), Bytes::from(bytes)))
    }

    pub fn decode_column(bytes: &[u8]) -> Result<ColumnCatalog, DatabaseError> {
        Ok(bincode::deserialize::<ColumnCatalog>(bytes)?)
    }

    /// Key: Root{BOUND_MIN_TAG}{TableName}
    /// Value: TableName
    pub fn encode_root_table(meta: &TableMeta) -> Result<(Bytes, Bytes), DatabaseError> {
        let key = Self::encode_root_table_key(&meta.table_name);

        Ok((Bytes::from(key), Bytes::from(bincode::serialize(meta)?)))
    }

    pub fn encode_root_table_key(table_name: &str) -> Vec<u8> {
        Self::key_prefix(CodecType::Root, table_name)
    }

    pub fn decode_root_table(bytes: &[u8]) -> Result<TableMeta, DatabaseError> {
        Ok(bincode::deserialize(bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ColumnCatalog, ColumnDesc, TableCatalog, TableMeta};
    use crate::errors::DatabaseError;
    use crate::storage::table_codec::TableCodec;
    use crate::types::index::{Index, IndexMeta};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use bytes::Bytes;
    use itertools::Itertools;
    use rust_decimal::Decimal;
    use std::collections::BTreeSet;
    use std::ops::Bound;
    use std::sync::Arc;

    fn build_table_codec() -> TableCatalog {
        let columns = vec![
            ColumnCatalog::new(
                "c1".into(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false, None),
                None,
            ),
            ColumnCatalog::new(
                "c2".into(),
                false,
                ColumnDesc::new(LogicalType::Decimal(None, None), false, false, None),
                None,
            ),
        ];
        TableCatalog::new(Arc::new("t1".to_string()), columns).unwrap()
    }

    #[test]
    fn test_table_codec_tuple() -> Result<(), DatabaseError> {
        let table_catalog = build_table_codec();

        let tuple = Tuple {
            id: Some(Arc::new(DataValue::Int32(Some(0)))),
            columns: Arc::new(table_catalog.all_columns()),
            values: vec![
                Arc::new(DataValue::Int32(Some(0))),
                Arc::new(DataValue::Decimal(Some(Decimal::new(1, 0)))),
            ],
        };
        let (_, bytes) = TableCodec::encode_tuple(&table_catalog.name, &tuple)?;
        let columns = Arc::new(table_catalog.all_columns().into_iter().collect_vec());

        assert_eq!(
            TableCodec::decode_tuple(&table_catalog.types(), &[0, 1], &columns, &bytes),
            tuple
        );

        Ok(())
    }

    #[test]
    fn test_root_catalog() {
        let table_catalog = build_table_codec();
        let (_, bytes) = TableCodec::encode_root_table(&TableMeta {
            colum_meta_paths: vec![],
            table_name: table_catalog.name.clone(),
        })
        .unwrap();

        let table_meta = TableCodec::decode_root_table(&bytes).unwrap();

        assert_eq!(table_meta.table_name.as_str(), table_catalog.name.as_str());
        assert!(table_meta.colum_meta_paths.is_empty());
    }

    #[test]
    fn test_table_codec_index_meta() -> Result<(), DatabaseError> {
        let index_meta = IndexMeta {
            id: 0,
            column_ids: vec![0],
            name: "index_1".to_string(),
            is_unique: false,
            is_primary: false,
        };
        let (_, bytes) = TableCodec::encode_index_meta(&"T1".to_string(), &index_meta)?;

        assert_eq!(TableCodec::decode_index_meta(&bytes)?, index_meta);

        Ok(())
    }

    #[test]
    fn test_table_codec_index() -> Result<(), DatabaseError> {
        let table_catalog = build_table_codec();

        let index = Index {
            id: 0,
            column_values: vec![Arc::new(DataValue::Int32(Some(0)))],
        };
        let tuple_ids = vec![Arc::new(DataValue::Int32(Some(0)))];
        let (_, bytes) = TableCodec::encode_index(&table_catalog.name, &index, &tuple_ids)?;

        assert_eq!(TableCodec::decode_index(&bytes)?, tuple_ids);

        Ok(())
    }

    #[test]
    fn test_table_codec_column() {
        let table_catalog = build_table_codec();
        let col = table_catalog.all_columns()[0].clone();

        let (_, bytes) = TableCodec::encode_column(&table_catalog.name, &col).unwrap();
        let decode_col = TableCodec::decode_column(&bytes).unwrap();

        assert_eq!(&decode_col, col.as_ref());
    }

    #[test]
    fn test_table_codec_column_bound() {
        let mut set = BTreeSet::new();
        let op = |col_id: usize, table_name: &str| {
            let mut col = ColumnCatalog::new(
                "".to_string(),
                false,
                ColumnDesc {
                    column_datatype: LogicalType::Invalid,
                    is_primary: false,
                    is_unique: false,
                    default: None,
                },
                None,
            );

            col.summary.id = Some(col_id as u32);

            let (key, _) = TableCodec::encode_column(&table_name.to_string(), &col).unwrap();
            key
        };

        set.insert(op(0, "T0"));
        set.insert(op(1, "T0"));
        set.insert(op(2, "T0"));

        set.insert(op(0, "T1"));
        set.insert(op(1, "T1"));
        set.insert(op(2, "T1"));

        set.insert(op(0, "T2"));
        set.insert(op(0, "T2"));
        set.insert(op(0, "T2"));

        let (min, max) = TableCodec::columns_bound(&Arc::new("T1".to_string()));

        let vec = set
            .range::<Bytes, (Bound<&Bytes>, Bound<&Bytes>)>((
                Bound::Included(&Bytes::from(min)),
                Bound::Included(&Bytes::from(max)),
            ))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(vec[0], &op(0, "T1"));
        assert_eq!(vec[1], &op(1, "T1"));
        assert_eq!(vec[2], &op(2, "T1"));
    }

    #[test]
    fn test_table_codec_index_meta_bound() {
        let mut set = BTreeSet::new();
        let op = |index_id: usize, table_name: &str| {
            let index_meta = IndexMeta {
                id: index_id as u32,
                column_ids: vec![],
                name: "".to_string(),
                is_unique: false,
                is_primary: false,
            };

            let (key, _) =
                TableCodec::encode_index_meta(&table_name.to_string(), &index_meta).unwrap();
            key
        };

        set.insert(op(0, "T0"));
        set.insert(op(1, "T0"));
        set.insert(op(2, "T0"));

        set.insert(op(0, "T1"));
        set.insert(op(1, "T1"));
        set.insert(op(2, "T1"));

        set.insert(op(0, "T2"));
        set.insert(op(1, "T2"));
        set.insert(op(2, "T2"));

        let (min, max) = TableCodec::index_meta_bound(&"T1".to_string());

        let vec = set
            .range::<Bytes, (Bound<&Bytes>, Bound<&Bytes>)>((
                Bound::Included(&Bytes::from(min)),
                Bound::Included(&Bytes::from(max)),
            ))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(vec[0], &op(0, "T1"));
        assert_eq!(vec[1], &op(1, "T1"));
        assert_eq!(vec[2], &op(2, "T1"));
    }

    #[test]
    fn test_table_codec_index_bound() {
        let mut set = BTreeSet::new();
        let column = ColumnCatalog::new(
            "".to_string(),
            false,
            ColumnDesc::new(LogicalType::Boolean, false, false, None),
            None,
        );
        let table_catalog = TableCatalog::new(Arc::new("T0".to_string()), vec![column]).unwrap();

        let op = |value: DataValue, index_id: usize, table_name: &String| {
            let index = Index {
                id: index_id as u32,
                column_values: vec![Arc::new(value)],
            };

            TableCodec::encode_index_key(table_name, &index).unwrap()
        };

        set.insert(op(DataValue::Int32(Some(0)), 0, &table_catalog.name));
        set.insert(op(DataValue::Int32(Some(1)), 0, &table_catalog.name));
        set.insert(op(DataValue::Int32(Some(2)), 0, &table_catalog.name));

        set.insert(op(DataValue::Int32(Some(0)), 1, &table_catalog.name));
        set.insert(op(DataValue::Int32(Some(1)), 1, &table_catalog.name));
        set.insert(op(DataValue::Int32(Some(2)), 1, &table_catalog.name));

        set.insert(op(DataValue::Int32(Some(0)), 2, &table_catalog.name));
        set.insert(op(DataValue::Int32(Some(1)), 2, &table_catalog.name));
        set.insert(op(DataValue::Int32(Some(2)), 2, &table_catalog.name));

        println!("{:#?}", set);

        let (min, max) = TableCodec::index_bound(&table_catalog.name, &1);

        println!("{:?}", min);
        println!("{:?}", max);

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(
            vec[0],
            &op(DataValue::Int32(Some(0)), 1, &table_catalog.name)
        );
        assert_eq!(
            vec[1],
            &op(DataValue::Int32(Some(1)), 1, &table_catalog.name)
        );
        assert_eq!(
            vec[2],
            &op(DataValue::Int32(Some(2)), 1, &table_catalog.name)
        );
    }

    #[test]
    fn test_table_codec_index_all_bound() {
        let mut set = BTreeSet::new();
        let op = |value: DataValue, index_id: usize, table_name: &str| {
            let index = Index {
                id: index_id as u32,
                column_values: vec![Arc::new(value)],
            };

            TableCodec::encode_index_key(&table_name.to_string(), &index).unwrap()
        };

        set.insert(op(DataValue::Int32(Some(0)), 0, "T0"));
        set.insert(op(DataValue::Int32(Some(1)), 0, "T0"));
        set.insert(op(DataValue::Int32(Some(2)), 0, "T0"));

        set.insert(op(DataValue::Int32(Some(0)), 0, "T1"));
        set.insert(op(DataValue::Int32(Some(1)), 0, "T1"));
        set.insert(op(DataValue::Int32(Some(2)), 0, "T1"));

        set.insert(op(DataValue::Int32(Some(0)), 0, "T2"));
        set.insert(op(DataValue::Int32(Some(1)), 0, "T2"));
        set.insert(op(DataValue::Int32(Some(2)), 0, "T2"));

        let (min, max) = TableCodec::all_index_bound(&"T1".to_string());

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(vec[0], &op(DataValue::Int32(Some(0)), 0, "T1"));
        assert_eq!(vec[1], &op(DataValue::Int32(Some(1)), 0, "T1"));
        assert_eq!(vec[2], &op(DataValue::Int32(Some(2)), 0, "T1"));
    }

    #[test]
    fn test_table_codec_tuple_bound() {
        let mut set = BTreeSet::new();
        let op = |tuple_id: DataValue, table_name: &str| {
            TableCodec::encode_tuple_key(&table_name.to_string(), &Arc::new(tuple_id)).unwrap()
        };

        set.insert(op(DataValue::Int32(Some(0)), "T0"));
        set.insert(op(DataValue::Int32(Some(1)), "T0"));
        set.insert(op(DataValue::Int32(Some(2)), "T0"));

        set.insert(op(DataValue::Int32(Some(0)), "T1"));
        set.insert(op(DataValue::Int32(Some(1)), "T1"));
        set.insert(op(DataValue::Int32(Some(2)), "T1"));

        set.insert(op(DataValue::Int32(Some(0)), "T2"));
        set.insert(op(DataValue::Int32(Some(1)), "T2"));
        set.insert(op(DataValue::Int32(Some(2)), "T2"));

        let (min, max) = TableCodec::tuple_bound(&"T1".to_string());

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(vec[0], &op(DataValue::Int32(Some(0)), "T1"));
        assert_eq!(vec[1], &op(DataValue::Int32(Some(1)), "T1"));
        assert_eq!(vec[2], &op(DataValue::Int32(Some(2)), "T1"));
    }

    #[test]
    fn test_root_codec_name_bound() {
        let mut set = BTreeSet::new();
        let op = |table_name: &str| TableCodec::encode_root_table_key(&table_name.to_string());

        set.insert(b"A".to_vec());

        set.insert(op("T0"));
        set.insert(op("T1"));
        set.insert(op("T2"));

        set.insert(b"Z".to_vec());

        let (min, max) = TableCodec::root_table_bound();

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec[0], &op("T0"));
        assert_eq!(vec[1], &op("T1"));
        assert_eq!(vec[2], &op("T2"));
    }
}
