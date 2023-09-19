use std::sync::Arc;
use bytes::Bytes;
use crate::catalog::{ColumnCatalog, TableCatalog, TableName};
use crate::types::errors::TypeError;
use crate::types::index::{Index, IndexId, IndexMeta};
use crate::types::tuple::{Tuple, TupleId};

const BOUND_MIN_TAG: u8 = 0;
const BOUND_MAX_TAG: u8 = 1;

const COLUMNS_ID_LEN: usize = 10;

#[derive(Clone)]
pub struct TableCodec {
    pub table: TableCatalog
}

impl TableCodec {
    pub fn tuple_bound(&self) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            format!(
                "{}_Tuple_{}",
                self.table.name,
                bound_id
            )
        };

        (op(BOUND_MIN_TAG).into_bytes(), op(BOUND_MAX_TAG).into_bytes())
    }

    pub fn index_meta_bound(name: &String) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            format!(
                "{}_IndexMeta_{}",
                name,
                bound_id
            )
        };

        (op(BOUND_MIN_TAG).into_bytes(), op(BOUND_MAX_TAG).into_bytes())
    }

    pub fn index_bound(&self, index_id: &IndexId) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            format!(
                "{}_Index_0_{}_{}",
                self.table.name,
                index_id,
                bound_id
            )
        };

        (op(BOUND_MIN_TAG).into_bytes(), op(BOUND_MAX_TAG).into_bytes())
    }

    pub fn all_index_bound(&self) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            format!(
                "{}_Index_{}",
                self.table.name,
                bound_id
            )
        };

        (op(BOUND_MIN_TAG).into_bytes(), op(BOUND_MAX_TAG).into_bytes())
    }

    pub fn columns_bound(name: &String) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            format!(
                "{}_Catalog_{}",
                name,
                bound_id
            )
        };

        (op(BOUND_MIN_TAG).into_bytes(), op(BOUND_MAX_TAG).into_bytes())
    }

    /// Key: TableName_Tuple_0_RowID(Sorted)
    /// Value: Tuple
    pub fn encode_tuple(&self, tuple: &Tuple) -> Result<(Bytes, Bytes), TypeError> {
        let tuple_id = tuple
            .id
            .clone()
            .ok_or(TypeError::NotNull)?;
        let key = self.encode_tuple_key(&tuple_id)?;

        Ok((Bytes::from(key), Bytes::from(tuple.serialize_to())))
    }

    pub fn encode_tuple_key(&self, tuple_id: &TupleId) -> Result<Vec<u8>, TypeError> {
        let string_key = format!(
            "{}_Tuple_0_{}",
            self.table.name,
            tuple_id.to_primary_key()?,
        );

        Ok(string_key.into_bytes())
    }

    pub fn decode_tuple(&self, bytes: &[u8]) -> Tuple {
        Tuple::deserialize_from(self.table.all_columns(), bytes)
    }

    /// Key: TableName_IndexMeta_0_IndexID
    /// Value: IndexMeta
    pub fn encode_index_meta(name: &String, index_meta: &IndexMeta) -> Result<(Bytes, Bytes), TypeError> {
        let key = format!(
            "{}_IndexMeta_0_{}",
            name,
            index_meta.id
        );

        Ok((Bytes::from(key), Bytes::from(bincode::serialize(&index_meta)?)))
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
    pub fn encode_index(&self, index: &Index, tuple_ids: &[TupleId]) -> Result<(Bytes, Bytes), TypeError> {
        let key = self.encode_index_key(index)?;

        Ok((Bytes::from(key), Bytes::from(bincode::serialize(tuple_ids)?)))
    }

    pub fn encode_index_key(&self, index: &Index) -> Result<Vec<u8>, TypeError> {
        let mut string_key = format!(
            "{}_Index_0_{}_0",
            self.table.name,
            index.id
        );

        for col_v in &index.column_values {
            string_key += &format!("_{}", col_v.to_index_key()?);
        }

        Ok(string_key.into_bytes())
    }

    pub fn decode_index(bytes: &[u8]) -> Result<Vec<TupleId>, TypeError> {
        Ok(bincode::deserialize(bytes)?)
    }

    /// Key: TableName_Catalog_0_ColumnName_ColumnId
    /// Value: ColumnCatalog
    ///
    /// Tips: the `0` for bound range
    pub fn encode_column(table_name: &String, col: &ColumnCatalog) -> Option<(Bytes, Bytes)> {
        bincode::serialize(col).ok()
            .map(|bytes| {
                let key = format!(
                    "{}_Catalog_{}_{}_{:0width$}",
                    table_name,
                    BOUND_MIN_TAG,
                    col.name,
                    col.id.unwrap(),
                    width = COLUMNS_ID_LEN
                );

                (Bytes::from(key.into_bytes()), Bytes::from(bytes))
            })
    }

    pub fn decode_column(key: &[u8], bytes: &[u8]) -> Option<(TableName, ColumnCatalog)> {
        String::from_utf8(key.to_owned()).ok()?
            .split("_")
            .nth(0)
            .and_then(|table_name| {
                bincode::deserialize::<ColumnCatalog>(bytes).ok()
                    .and_then(|col| {
                        Some((Arc::new(table_name.to_string()), col))
                    })
            })
    }


    /// Key: RootCatalog_0_TableName
    /// Value: ColumnCount
    pub fn encode_root_table(table_name: &str,column_count:usize) -> Option<(Bytes, Bytes)> {
        let key = format!(
            "RootCatalog_{}_{}",
            BOUND_MIN_TAG,
            table_name,
        );

        bincode::serialize(&column_count).ok()
            .map(|bytes| {
                (Bytes::from(key.into_bytes()), Bytes::from(bytes))
            })
    }

    // TODO: value is reserved for saving meta-information
    pub fn decode_root_table(key: &[u8], bytes: &[u8]) -> Option<(String,usize)> {
        String::from_utf8(key.to_owned()).ok()?
            .split("_")
            .nth(2)
            .and_then(|table_name| {
                bincode::deserialize::<usize>(bytes).ok()
                    .and_then(|name| {
                        Some((table_name.to_string(), name))
                    })
            })
    }

    pub fn root_table_bound() -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            format!(
                "RootCatalog_{}",
                bound_id,
            )
        };

        (op(BOUND_MIN_TAG).into_bytes(), op(BOUND_MAX_TAG).into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::ops::Bound;
    use std::sync::Arc;
    use itertools::Itertools;
    use crate::catalog::{ColumnCatalog, ColumnDesc, TableCatalog};
    use crate::storage::table_codec::{COLUMNS_ID_LEN, TableCodec};
    use crate::types::errors::TypeError;
    use crate::types::index::{Index, IndexMeta};
    use crate::types::LogicalType;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;

    fn build_table_codec() -> (TableCatalog, TableCodec) {
        let columns = vec![
            ColumnCatalog::new(
                "c1".into(),
                false,
                ColumnDesc::new(LogicalType::Integer, true, false)
            )
        ];
        let table_catalog = TableCatalog::new(Arc::new("t1".to_string()), columns, vec![]).unwrap();
        let codec = TableCodec { table: table_catalog.clone() };
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
            ]
        };

        let (key, bytes) = codec.encode_tuple(&tuple)?;

        assert_eq!(
            String::from_utf8(key.to_vec()).ok().unwrap(),
            format!(
                "{}_Tuple_0_{}",
                table_catalog.name,
                tuple.id.clone().unwrap().to_primary_key()?,
            )
        );
        assert_eq!(codec.decode_tuple(&bytes), tuple);

        Ok(())
    }

    #[test]
    fn test_root_catalog() {
        let (table_catalog, _) = build_table_codec();
        let (key, bytes) = TableCodec::encode_root_table(&table_catalog.name,2).unwrap();

        assert_eq!(
            String::from_utf8(key.to_vec()).ok().unwrap(),
            format!(
                "RootCatalog_0_{}",
                table_catalog.name,
            )
        );

        let (table_name, column_count) = TableCodec::decode_root_table(&key, &bytes).unwrap();

        assert_eq!(table_name, table_catalog.name.as_str());
        assert_eq!(column_count, 2);
    }

    #[test]
    fn test_table_codec_index_meta() -> Result<(), TypeError> {
        let index_meta = IndexMeta {
            id: 0,
            column_ids: vec![0],
            name: "index_1".to_string(),
            is_unique: false,
        };

        let (key, bytes) = TableCodec::encode_index_meta(&"T1".to_string(), &index_meta)?;

        assert_eq!(
            String::from_utf8(key.to_vec()).ok().unwrap(),
            format!(
                "T1_IndexMeta_0_{}",
                index_meta.id
            )
        );
        assert_eq!(TableCodec::decode_index_meta(&bytes)?, index_meta);

        Ok(())
    }

    #[test]
    fn test_table_codec_index() -> Result<(), TypeError> {
        let (table_catalog, codec) = build_table_codec();

        let index = Index {
            id: 0,
            column_values: vec![Arc::new(DataValue::Int32(Some(0)))],
        };
        let tuple_ids = vec![Arc::new(DataValue::Int32(Some(0)))];

        let (key, bytes) = codec.encode_index(&index, &tuple_ids)?;

        assert_eq!(
            String::from_utf8(key.to_vec()).ok().unwrap(),
            format!(
                "{}_Index_0_{}_0_{}",
                table_catalog.name,
                index.id,
                index.column_values[0].to_index_key()?
            )
        );
        assert_eq!(TableCodec::decode_index(&bytes)?, tuple_ids);

        Ok(())
    }

    #[test]
    fn test_table_codec_column() {
        let (table_catalog, _) = build_table_codec();
        let col = table_catalog.all_columns()[0].clone();
        let (key, bytes) = TableCodec::encode_column(&table_catalog.name, &col).unwrap();

        assert_eq!(
            String::from_utf8(key.to_vec()).ok().unwrap(),
            format!(
                "{}_Catalog_0_{}_{:0width$}",
                table_catalog.name,
                col.name,
                col.id.unwrap(),
                width = COLUMNS_ID_LEN
            )
        );

        let (table_name, decode_col) = TableCodec::decode_column(&key, &bytes).unwrap();

        assert_eq!(&decode_col, col.as_ref());
        assert_eq!(table_name, table_catalog.name);
    }

    #[test]
    fn test_table_codec_column_bound() {
        let mut set = BTreeSet::new();
        let op = |str: &str| {
            str.to_string().into_bytes()
        };

        set.insert(op("T0_Catalog_0_C0_0"));
        set.insert(op("T0_Catalog_0_C1_1"));
        set.insert(op("T0_Catalog_0_C2_2"));

        set.insert(op("T1_Catalog_0_C0_0"));
        set.insert(op("T1_Catalog_0_C1_1"));
        set.insert(op("T1_Catalog_0_C2_2"));

        set.insert(op("T2_Catalog_0_C0_0"));
        set.insert(op("T2_Catalog_0_C1_1"));
        set.insert(op("T2_Catalog_0_C2_2"));

        let (min, max) = TableCodec::columns_bound(
            &Arc::new("T1".to_string())
        );

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((Bound::Included(&min), Bound::Included(&max)))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(String::from_utf8(vec[0].clone()).unwrap(), "T1_Catalog_0_C0_0");
        assert_eq!(String::from_utf8(vec[1].clone()).unwrap(), "T1_Catalog_0_C1_1");
        assert_eq!(String::from_utf8(vec[2].clone()).unwrap(), "T1_Catalog_0_C2_2");
    }

    #[test]
    fn test_table_codec_index_meta_bound() {
        let mut set = BTreeSet::new();
        let op = |str: &str| {
            str.to_string().into_bytes()
        };

        set.insert(op("T0_IndexMeta_0_0"));
        set.insert(op("T0_IndexMeta_0_1"));
        set.insert(op("T0_IndexMeta_0_2"));

        set.insert(op("T1_IndexMeta_0_0"));
        set.insert(op("T1_IndexMeta_0_1"));
        set.insert(op("T1_IndexMeta_0_2"));

        set.insert(op("T2_IndexMeta_0_0"));
        set.insert(op("T2_IndexMeta_0_1"));
        set.insert(op("T2_IndexMeta_0_2"));

        let (min, max) = TableCodec::index_meta_bound(&"T1".to_string());

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((Bound::Included(&min), Bound::Included(&max)))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(String::from_utf8(vec[0].clone()).unwrap(), "T1_IndexMeta_0_0");
        assert_eq!(String::from_utf8(vec[1].clone()).unwrap(), "T1_IndexMeta_0_1");
        assert_eq!(String::from_utf8(vec[2].clone()).unwrap(), "T1_IndexMeta_0_2");
    }

    #[test]
    fn test_table_codec_index_bound() {
        let mut set = BTreeSet::new();
        let op = |str: &str| {
            str.to_string().into_bytes()
        };

        set.insert(op("T0_Index_0_0_0_0000000000000000000"));
        set.insert(op("T0_Index_0_0_0_0000000000000000001"));
        set.insert(op("T0_Index_0_0_0_0000000000000000002"));

        set.insert(op("T0_Index_0_1_0_0000000000000000000"));
        set.insert(op("T0_Index_0_1_0_0000000000000000001"));
        set.insert(op("T0_Index_0_1_0_0000000000000000002"));

        set.insert(op("T0_Index_0_2_0_0000000000000000000"));
        set.insert(op("T0_Index_0_2_0_0000000000000000001"));
        set.insert(op("T0_Index_0_2_0_0000000000000000002"));

        let table_codec = TableCodec {
            table: TableCatalog::new(Arc::new("T0".to_string()), vec![], vec![]).unwrap(),
        };
        let (min, max) = table_codec.index_bound(&1);

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((Bound::Included(&min), Bound::Included(&max)))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(String::from_utf8(vec[0].clone()).unwrap(), "T0_Index_0_1_0_0000000000000000000");
        assert_eq!(String::from_utf8(vec[1].clone()).unwrap(), "T0_Index_0_1_0_0000000000000000001");
        assert_eq!(String::from_utf8(vec[2].clone()).unwrap(), "T0_Index_0_1_0_0000000000000000002");
    }

    #[test]
    fn test_table_codec_index_all_bound() {
        let mut set = BTreeSet::new();
        let op = |str: &str| {
            str.to_string().into_bytes()
        };

        set.insert(op("T0_Index_0_0_0_0000000000000000000"));
        set.insert(op("T0_Index_0_0_0_0000000000000000001"));
        set.insert(op("T0_Index_0_0_0_0000000000000000002"));

        set.insert(op("T1_Index_0_1_0_0000000000000000000"));
        set.insert(op("T1_Index_0_1_0_0000000000000000001"));
        set.insert(op("T1_Index_0_1_0_0000000000000000002"));

        set.insert(op("T2_Index_0_2_0_0000000000000000000"));
        set.insert(op("T2_Index_0_2_0_0000000000000000001"));
        set.insert(op("T2_Index_0_2_0_0000000000000000002"));

        let table_codec = TableCodec {
            table: TableCatalog::new(Arc::new("T1".to_string()), vec![], vec![]).unwrap(),
        };
        let (min, max) = table_codec.all_index_bound();

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((Bound::Included(&min), Bound::Included(&max)))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(String::from_utf8(vec[0].clone()).unwrap(), "T1_Index_0_1_0_0000000000000000000");
        assert_eq!(String::from_utf8(vec[1].clone()).unwrap(), "T1_Index_0_1_0_0000000000000000001");
        assert_eq!(String::from_utf8(vec[2].clone()).unwrap(), "T1_Index_0_1_0_0000000000000000002");
    }

    #[test]
    fn test_table_codec_tuple_bound() {
        let mut set = BTreeSet::new();
        let op = |str: &str| {
            str.to_string().into_bytes()
        };

        set.insert(op("T0_Tuple_0_0000000000000000000"));
        set.insert(op("T0_Tuple_0_0000000000000000001"));
        set.insert(op("T0_Tuple_0_0000000000000000002"));

        set.insert(op("T1_Tuple_0_0000000000000000000"));
        set.insert(op("T1_Tuple_0_0000000000000000001"));
        set.insert(op("T1_Tuple_0_0000000000000000002"));

        set.insert(op("T2_Tuple_0_0000000000000000000"));
        set.insert(op("T2_Tuple_0_0000000000000000001"));
        set.insert(op("T2_Tuple_0_0000000000000000002"));

        let table_codec = TableCodec {
            table: TableCatalog::new(Arc::new("T1".to_string()), vec![], vec![]).unwrap(),
        };
        let (min, max) = table_codec.tuple_bound();

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((Bound::Included(&min), Bound::Included(&max)))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(String::from_utf8(vec[0].clone()).unwrap(), "T1_Tuple_0_0000000000000000000");
        assert_eq!(String::from_utf8(vec[1].clone()).unwrap(), "T1_Tuple_0_0000000000000000001");
        assert_eq!(String::from_utf8(vec[2].clone()).unwrap(), "T1_Tuple_0_0000000000000000002");
    }

    #[test]
    fn test_root_codec_name_bound(){
        let mut set = BTreeSet::new();
        let op = |str: &str| {
            str.to_string().into_bytes()
        };

        set.insert(op("RootCatalog_0_T0"));
        set.insert(op("RootCatalog_0_T1"));
        set.insert(op("RootCatalog_0_T2"));

        let (min, max) = TableCodec::root_table_bound();

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((Bound::Included(&min), Bound::Included(&max)))
            .collect_vec();

        assert_eq!(String::from_utf8(vec[0].clone()).unwrap(), "RootCatalog_0_T0");
        assert_eq!(String::from_utf8(vec[1].clone()).unwrap(), "RootCatalog_0_T1");
        assert_eq!(String::from_utf8(vec[2].clone()).unwrap(), "RootCatalog_0_T2");

    }
}