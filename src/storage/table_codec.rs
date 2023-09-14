use std::sync::Arc;
use bytes::Bytes;
use crate::catalog::{ColumnCatalog, ColumnRef, TableCatalog, TableName};
use crate::types::errors::TypeError;
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
                "{}_Data_{}",
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

    /// Key: TableName_Data_0_RowID(Sorted)
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
            "{}_Data_0_{}",
            self.table.name,
            tuple_id.to_primary_key()?,
        );

        Ok(string_key.into_bytes())
    }

    pub fn decode_tuple(&self, bytes: &[u8]) -> Tuple {
        Tuple::deserialize_from(self.table.all_columns(), bytes)
    }

    /// Key: TableName_Catalog_0_ColumnName_ColumnId
    /// Value: ColumnCatalog
    ///
    /// Tips: the `0` for bound range
    pub fn encode_column(col: &ColumnRef) -> Option<(Bytes, Bytes)> {
        let table_name = col.table_name.as_ref()?;

        bincode::serialize(&col).ok()
            .map(|bytes| {
                let key = format!(
                    "{}_Catalog_{}_{}_{:0width$}",
                    table_name,
                    BOUND_MIN_TAG,
                    col.name,
                    col.id,
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
    use crate::types::LogicalType;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;

    fn build_table_codec() -> (TableCatalog, TableCodec) {
        let columns = vec![
            ColumnCatalog::new(
                "c1".into(),
                false,
                ColumnDesc::new(LogicalType::Integer, true)
            )
        ];
        let table_catalog = TableCatalog::new(Arc::new("t1".to_string()), columns).unwrap();
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
                "{}_Data_0_{}",
                table_catalog.name,
                tuple.id.clone().unwrap().to_primary_key()?,
            )
        );
        assert_eq!(codec.decode_tuple(&bytes), tuple);

        Ok(())
    }

    #[test]
    fn test_table_codec_column() {
        let (table_catalog, _) = build_table_codec();
        let col = table_catalog.all_columns()[0].clone();
        let (key, bytes) = TableCodec::encode_column(&col).unwrap();

        assert_eq!(
            String::from_utf8(key.to_vec()).ok().unwrap(),
            format!(
                "{}_Catalog_0_{}_{:0width$}",
                table_catalog.name,
                col.name,
                col.id,
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

        assert_eq!(String::from_utf8(vec[0].clone()).unwrap(), "T1_Catalog_0_C0_0");
        assert_eq!(String::from_utf8(vec[1].clone()).unwrap(), "T1_Catalog_0_C1_1");
        assert_eq!(String::from_utf8(vec[2].clone()).unwrap(), "T1_Catalog_0_C2_2");
    }

    #[test]
    fn test_table_codec_tuple_bound() {
        let mut set = BTreeSet::new();
        let op = |str: &str| {
            str.to_string().into_bytes()
        };

        set.insert(op("T0_Data_0_0000000000000000000"));
        set.insert(op("T0_Data_0_0000000000000000001"));
        set.insert(op("T0_Data_0_0000000000000000002"));

        set.insert(op("T1_Data_0_0000000000000000000"));
        set.insert(op("T1_Data_0_0000000000000000001"));
        set.insert(op("T1_Data_0_0000000000000000002"));

        set.insert(op("T2_Data_0_0000000000000000000"));
        set.insert(op("T2_Data_0_0000000000000000001"));
        set.insert(op("T2_Data_0_0000000000000000002"));

        let table_codec = TableCodec {
            table: TableCatalog::new(Arc::new("T1".to_string()), vec![]).unwrap(),
        };
        let (min, max) = table_codec.tuple_bound();

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((Bound::Included(&min), Bound::Included(&max)))
            .collect_vec();

        assert_eq!(String::from_utf8(vec[0].clone()).unwrap(), "T1_Data_0_0000000000000000000");
        assert_eq!(String::from_utf8(vec[1].clone()).unwrap(), "T1_Data_0_0000000000000000001");
        assert_eq!(String::from_utf8(vec[2].clone()).unwrap(), "T1_Data_0_0000000000000000002");
    }
}