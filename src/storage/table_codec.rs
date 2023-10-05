use crate::catalog::{ColumnCatalog, TableCatalog, TableName};
use crate::types::errors::TypeError;
use crate::types::index::{Index, IndexId, IndexMeta};
use crate::types::tuple::{Tuple, TupleId};
use bytes::Bytes;
use lazy_static::lazy_static;

const BOUND_MIN_TAG: u8 = 0;
const BOUND_MAX_TAG: u8 = 1;
lazy_static! {
    static ref ROOT_BYTES: Vec<u8> = b"Root".to_vec();
}

#[derive(Clone)]
pub struct TableCodec {
    pub table: TableCatalog,
}

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
    fn key_prefix(ty: CodecType, table_name: &String) -> Vec<u8> {
        let mut table_bytes = table_name.clone().into_bytes();

        match ty {
            CodecType::Column => {
                table_bytes.push(b'0');
            }
            CodecType::IndexMeta => {
                table_bytes.push(b'1');
            }
            CodecType::Index => {
                table_bytes.push(b'2');
            }
            CodecType::Tuple => {
                table_bytes.push(b'3');
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

    pub fn tuple_bound(&self) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            let mut key_prefix = Self::key_prefix(CodecType::Tuple, &self.table.name);

            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn index_meta_bound(name: &String) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            let mut key_prefix = Self::key_prefix(CodecType::IndexMeta, name);

            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn index_bound(&self, index_id: &IndexId) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            let mut key_prefix = Self::key_prefix(CodecType::Index, &self.table.name);

            key_prefix.push(BOUND_MIN_TAG);
            key_prefix.append(&mut index_id.to_be_bytes().to_vec());
            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn all_index_bound(&self) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            let mut key_prefix = Self::key_prefix(CodecType::Index, &self.table.name);

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

    pub fn columns_bound(name: &String) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            let mut key_prefix = Self::key_prefix(CodecType::Column, &name);

            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    /// Key: TableName_Tuple_0_RowID(Sorted)
    /// Value: Tuple
    pub fn encode_tuple(&self, tuple: &Tuple) -> Result<(Bytes, Bytes), TypeError> {
        let tuple_id = tuple.id.clone().ok_or(TypeError::NotNull)?;
        let key = self.encode_tuple_key(&tuple_id)?;

        Ok((Bytes::from(key), Bytes::from(tuple.serialize_to())))
    }

    pub fn encode_tuple_key(&self, tuple_id: &TupleId) -> Result<Vec<u8>, TypeError> {
        let mut key_prefix = Self::key_prefix(CodecType::Tuple, &self.table.name);
        key_prefix.push(BOUND_MIN_TAG);

        tuple_id.to_primary_key(&mut key_prefix)?;

        Ok(key_prefix)
    }

    pub fn decode_tuple(&self, bytes: &[u8]) -> Tuple {
        Tuple::deserialize_from(self.table.all_columns(), bytes)
    }

    /// Key: TableName_IndexMeta_0_IndexID
    /// Value: IndexMeta
    pub fn encode_index_meta(
        name: &String,
        index_meta: &IndexMeta,
    ) -> Result<(Bytes, Bytes), TypeError> {
        let mut key_prefix = Self::key_prefix(CodecType::IndexMeta, &name);
        key_prefix.push(BOUND_MIN_TAG);
        key_prefix.append(&mut index_meta.id.to_be_bytes().to_vec());

        Ok((
            Bytes::from(key_prefix),
            Bytes::from(bincode::serialize(&index_meta)?),
        ))
    }

    pub fn decode_index_meta(bytes: &[u8]) -> Result<IndexMeta, TypeError> {
        Ok(bincode::deserialize(bytes)?)
    }

    /// NonUnique Index:
    /// Key: TableName_Index_0_IndexID_0_DataValue1_DataValue2 ..
    /// Value: TupleIDs
    ///
    /// Unique Index:
    /// Key: TableName_Index_0_IndexID_0_DataValue
    /// Value: TupleIDs
    ///
    /// Tips: The unique index has only one ColumnID and one corresponding DataValue,
    /// so it can be positioned directly.
    pub fn encode_index(
        &self,
        index: &Index,
        tuple_ids: &[TupleId],
    ) -> Result<(Bytes, Bytes), TypeError> {
        let key = self.encode_index_key(index)?;

        Ok((
            Bytes::from(key),
            Bytes::from(bincode::serialize(tuple_ids)?),
        ))
    }

    pub fn encode_index_key(&self, index: &Index) -> Result<Vec<u8>, TypeError> {
        let mut key_prefix = Self::key_prefix(CodecType::Index, &self.table.name);
        key_prefix.push(BOUND_MIN_TAG);
        key_prefix.append(&mut index.id.to_be_bytes().to_vec());
        key_prefix.push(BOUND_MIN_TAG);

        for col_v in &index.column_values {
            col_v.to_index_key(&mut key_prefix)?;
        }

        Ok(key_prefix)
    }

    pub fn decode_index(bytes: &[u8]) -> Result<Vec<TupleId>, TypeError> {
        Ok(bincode::deserialize(bytes)?)
    }

    /// Key: TableName_Catalog_0_ColumnName_ColumnId
    /// Value: ColumnCatalog
    ///
    /// Tips: the `0` for bound range
    pub fn encode_column(col: &ColumnCatalog) -> Result<(Bytes, Bytes), TypeError> {
        let bytes = bincode::serialize(col)?;
        let mut key_prefix = Self::key_prefix(CodecType::Column, col.table_name.as_ref().unwrap());

        key_prefix.push(BOUND_MIN_TAG);
        key_prefix.append(&mut col.id.unwrap().to_be_bytes().to_vec());

        Ok((Bytes::from(key_prefix), Bytes::from(bytes)))
    }

    pub fn decode_column(bytes: &[u8]) -> Result<(TableName, ColumnCatalog), TypeError> {
        let column = bincode::deserialize::<ColumnCatalog>(bytes)?;

        Ok((column.table_name.clone().unwrap(), column))
    }

    /// Key: RootCatalog_0_TableName
    /// Value: TableName
    pub fn encode_root_table(table_name: &String) -> Result<(Bytes, Bytes), TypeError> {
        let key = Self::encode_root_table_key(table_name);

        Ok((
            Bytes::from(key),
            Bytes::from(table_name.clone().into_bytes()),
        ))
    }

    pub fn encode_root_table_key(table_name: &String) -> Vec<u8> {
        Self::key_prefix(CodecType::Root, &table_name)
    }

    pub fn decode_root_table(bytes: &[u8]) -> Result<String, TypeError> {
        Ok(String::from_utf8(bytes.to_vec())?)
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ColumnCatalog, ColumnDesc, TableCatalog};
    use crate::storage::table_codec::TableCodec;
    use crate::types::errors::TypeError;
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

    fn build_table_codec() -> (TableCatalog, TableCodec) {
        let columns = vec![
            ColumnCatalog::new(
                "c1".into(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false),
                None,
            ),
            ColumnCatalog::new(
                "c2".into(),
                false,
                ColumnDesc::new(LogicalType::Decimal(None, None), false, false),
                None,
            ),
        ];
        let table_catalog = TableCatalog::new(Arc::new("t1".to_string()), columns).unwrap();
        let codec = TableCodec {
            table: table_catalog.clone(),
        };
        (table_catalog, codec)
    }

    #[test]
    fn test_table_codec_tuple() -> Result<(), TypeError> {
        let (table_catalog, codec) = build_table_codec();

        let tuple = Tuple {
            id: Some(Arc::new(DataValue::Int32(Some(0)))),
            columns: table_catalog.all_columns(),
            values: vec![
                Arc::new(DataValue::Int32(Some(0))),
                Arc::new(DataValue::Decimal(Some(Decimal::new(1, 0)))),
            ],
        };
        let (_, bytes) = codec.encode_tuple(&tuple)?;

        assert_eq!(codec.decode_tuple(&bytes), tuple);

        Ok(())
    }

    #[test]
    fn test_root_catalog() {
        let (table_catalog, _) = build_table_codec();
        let (_, bytes) = TableCodec::encode_root_table(&table_catalog.name).unwrap();

        let table_name = TableCodec::decode_root_table(&bytes).unwrap();

        assert_eq!(table_name, table_catalog.name.as_str());
    }

    #[test]
    fn test_table_codec_index_meta() -> Result<(), TypeError> {
        let index_meta = IndexMeta {
            id: 0,
            column_ids: vec![0],
            name: "index_1".to_string(),
            is_unique: false,
        };
        let (_, bytes) = TableCodec::encode_index_meta(&"T1".to_string(), &index_meta)?;

        assert_eq!(TableCodec::decode_index_meta(&bytes)?, index_meta);

        Ok(())
    }

    #[test]
    fn test_table_codec_index() -> Result<(), TypeError> {
        let (_, codec) = build_table_codec();

        let index = Index {
            id: 0,
            column_values: vec![Arc::new(DataValue::Int32(Some(0)))],
        };
        let tuple_ids = vec![Arc::new(DataValue::Int32(Some(0)))];
        let (_, bytes) = codec.encode_index(&index, &tuple_ids)?;

        assert_eq!(TableCodec::decode_index(&bytes)?, tuple_ids);

        Ok(())
    }

    #[test]
    fn test_table_codec_column() {
        let (table_catalog, _) = build_table_codec();
        let col = table_catalog.all_columns()[0].clone();
        let (_, bytes) = TableCodec::encode_column(&col).unwrap();

        let (table_name, decode_col) = TableCodec::decode_column(&bytes).unwrap();

        assert_eq!(&decode_col, col.as_ref());
        assert_eq!(table_name, table_catalog.name);
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
                },
                None,
            );

            col.table_name = Some(Arc::new(table_name.to_string()));
            col.id = Some(col_id as u32);

            let (key, _) = TableCodec::encode_column(&col).unwrap();
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
        let table_codec = TableCodec {
            table: TableCatalog::new(Arc::new("T0".to_string()), vec![]).unwrap(),
        };

        let op = |value: DataValue, index_id: usize, table_codec: &TableCodec| {
            let index = Index {
                id: index_id as u32,
                column_values: vec![Arc::new(value)],
            };

            table_codec.encode_index_key(&index).unwrap()
        };

        set.insert(op(DataValue::Int32(Some(0)), 0, &table_codec));
        set.insert(op(DataValue::Int32(Some(1)), 0, &table_codec));
        set.insert(op(DataValue::Int32(Some(2)), 0, &table_codec));

        set.insert(op(DataValue::Int32(Some(0)), 1, &table_codec));
        set.insert(op(DataValue::Int32(Some(1)), 1, &table_codec));
        set.insert(op(DataValue::Int32(Some(2)), 1, &table_codec));

        set.insert(op(DataValue::Int32(Some(0)), 2, &table_codec));
        set.insert(op(DataValue::Int32(Some(1)), 2, &table_codec));
        set.insert(op(DataValue::Int32(Some(2)), 2, &table_codec));

        println!("{:#?}", set);

        let (min, max) = table_codec.index_bound(&1);

        println!("{:?}", min);
        println!("{:?}", max);

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(vec[0], &op(DataValue::Int32(Some(0)), 1, &table_codec));
        assert_eq!(vec[1], &op(DataValue::Int32(Some(1)), 1, &table_codec));
        assert_eq!(vec[2], &op(DataValue::Int32(Some(2)), 1, &table_codec));
    }

    #[test]
    fn test_table_codec_index_all_bound() {
        let mut set = BTreeSet::new();
        let op = |value: DataValue, index_id: usize, table_name: &str| {
            let index = Index {
                id: index_id as u32,
                column_values: vec![Arc::new(value)],
            };

            TableCodec {
                table: TableCatalog::new(Arc::new(table_name.to_string()), vec![]).unwrap(),
            }
            .encode_index_key(&index)
            .unwrap()
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

        let table_codec = TableCodec {
            table: TableCatalog::new(Arc::new("T1".to_string()), vec![]).unwrap(),
        };
        let (min, max) = table_codec.all_index_bound();

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
            TableCodec {
                table: TableCatalog::new(Arc::new(table_name.to_string()), vec![]).unwrap(),
            }
            .encode_tuple_key(&Arc::new(tuple_id))
            .unwrap()
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

        let table_codec = TableCodec {
            table: TableCatalog::new(Arc::new("T1".to_string()), vec![]).unwrap(),
        };
        let (min, max) = table_codec.tuple_bound();

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
