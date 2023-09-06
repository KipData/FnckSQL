use std::sync::Arc;
use bytes::Bytes;
use crate::catalog::{ColumnRef, TableCatalog, TableName};
use crate::types::tuple::Tuple;

const COLUMNS_MIN: u8 = 0;
const COLUMNS_MAX: u8 = 1;

#[derive(Clone)]
pub struct TableCodec {
    pub table: TableCatalog
}

impl TableCodec {
    pub fn tuple_bound(&self) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            format!(
                "Data_{}_{:0width$}",
                self.table.name,
                bound_id,
                width = std::mem::size_of::<usize>() * 2 - 6
            )
        };

        (op(0_u32).into_bytes(), op(u32::MAX).into_bytes())
    }

    pub fn columns_bound(name: &TableName) -> (Vec<u8>, Vec<u8>) {
        let op = |bound_id| {
            format!(
                "Catalog_{}_{}",
                name,
                bound_id
            )
        };

        (op(COLUMNS_MIN).into_bytes(), op(COLUMNS_MAX).into_bytes())
    }

    /// Key: Data_TableName_RowID(Sorted)
    /// Value: Tuple
    pub fn encode_tuple(&self, tuple: &Tuple) -> (Bytes, Bytes) {
        let key = format!(
            "Data_{}_{:0width$}",
            self.table.name,
            tuple.id.unwrap(),
            width = std::mem::size_of::<usize>() * 2 - 6
        );
        (Bytes::from(key.into_bytes()), Bytes::from(tuple.serialize_to()))
    }

    pub fn decode_tuple(&self, key: &[u8], bytes: &[u8]) -> Option<Tuple> {
        String::from_utf8(key.to_owned()).ok()?
            .split("_")
            .nth(2)
            .and_then(|last| last.parse::<usize>().ok()
                .map(|row_id| {
                    Tuple::deserialize_from(
                        Some(row_id),
                        self.table.all_columns(),
                        bytes
                    )
                }))
    }

    /// Key: Catalog_TableName_0_ColumnName_ColumnId
    /// Value: TableCatalog
    ///
    /// Tips: the `0` for bound range
    pub fn encode_column(col: &ColumnRef) -> Option<(Bytes, Bytes)> {
        let table_name = col.table_name.as_ref()?;

        bincode::serialize(&col).ok()
            .map(|bytes| {
                let key = format!(
                    "Catalog_{}_{}_{}_{:0width$}",
                    table_name,
                    COLUMNS_MIN,
                    col.name,
                    col.id,
                    width = std::mem::size_of::<usize>() * 2 - 6
                );

                (Bytes::from(key.into_bytes()), Bytes::from(bytes))
            })
    }

    pub fn decode_column(key: &[u8], bytes: &[u8]) -> Option<(TableName, ColumnRef)> {
        String::from_utf8(key.to_owned()).ok()?
            .split("_")
            .nth(1)
            .and_then(|table_name| {
                bincode::deserialize::<ColumnRef>(bytes).ok()
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
    use crate::storage::table_codec::TableCodec;
    use crate::types::LogicalType;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;

    fn build_table_codec() -> (TableCatalog, TableCodec) {
        let columns = vec![
            ColumnCatalog::new(
                "c1".into(),
                false,
                ColumnDesc::new(LogicalType::Integer, false)
            )
        ];
        let table_catalog = TableCatalog::new(Arc::new("t1".to_string()), columns).unwrap();
        let codec = TableCodec { table: table_catalog.clone() };
        (table_catalog, codec)
    }

    #[test]
    fn test_table_codec_tuple() {
        let (table_catalog, codec) = build_table_codec();

        let tuple = Tuple {
            id: Some(0),
            columns: table_catalog.all_columns(),
            values: vec![
                Arc::new(DataValue::Int32(Some(0))),
            ]
        };

        let (key, bytes) = codec.encode_tuple(&tuple);

        assert_eq!(
            String::from_utf8(key.to_vec()).ok().unwrap(),
            format!(
                "Data_{}_{:0width$}",
                table_catalog.name,
                tuple.id.unwrap(),
                width = std::mem::size_of::<usize>() * 2 - 6
            )
        );
        assert_eq!(codec.decode_tuple(&key, &bytes).unwrap(), tuple)
    }

    #[test]
    fn test_table_codec_column() {
        let (table_catalog, _) = build_table_codec();
        let col = table_catalog.all_columns()[0].clone();
        let (key, bytes) = TableCodec::encode_column(&col).unwrap();

        assert_eq!(
            String::from_utf8(key.to_vec()).ok().unwrap(),
            format!(
                "Catalog_{}_0_{}_{:0width$}",
                table_catalog.name,
                col.name,
                col.id,
                width = std::mem::size_of::<usize>() * 2 - 6
            )
        );

        let (table_name, decode_col) = TableCodec::decode_column(&key, &bytes).unwrap();

        assert_eq!(decode_col, col);
        assert_eq!(table_name, table_catalog.name);
    }

    #[test]
    fn test_table_codec_column_bound() {
        let mut set = BTreeSet::new();
        let op = |str: &str| {
            str.to_string().into_bytes()
        };

        set.insert(op("Catalog_T0_0_C0_0"));
        set.insert(op("Catalog_T0_0_C1_1"));
        set.insert(op("Catalog_T0_0_C2_2"));

        set.insert(op("Catalog_T1_0_C0_0"));
        set.insert(op("Catalog_T1_0_C1_1"));
        set.insert(op("Catalog_T1_0_C2_2"));

        set.insert(op("Catalog_T2_0_C0_0"));
        set.insert(op("Catalog_T2_0_C1_1"));
        set.insert(op("Catalog_T2_0_C2_2"));

        let (min, max) = TableCodec::columns_bound(
            &Arc::new("T1".to_string())
        );

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((Bound::Included(&min), Bound::Included(&max)))
            .collect_vec();

        assert_eq!(String::from_utf8(vec[0].clone()).unwrap(), "Catalog_T1_0_C0_0");
        assert_eq!(String::from_utf8(vec[1].clone()).unwrap(), "Catalog_T1_0_C1_1");
        assert_eq!(String::from_utf8(vec[2].clone()).unwrap(), "Catalog_T1_0_C2_2");
    }

    #[test]
    fn test_table_codec_tuple_bound() {
        let mut set = BTreeSet::new();
        let op = |str: &str| {
            str.to_string().into_bytes()
        };

        set.insert(op("Data_T0_0000000000"));
        set.insert(op("Data_T0_0000000001"));
        set.insert(op("Data_T0_0000000002"));

        set.insert(op("Data_T1_0000000000"));
        set.insert(op("Data_T1_0000000001"));
        set.insert(op("Data_T1_0000000002"));

        set.insert(op("Data_T2_0000000000"));
        set.insert(op("Data_T2_0000000001"));
        set.insert(op("Data_T2_0000000002"));

        let table_codec = TableCodec {
            table: TableCatalog::new(Arc::new("T1".to_string()), vec![]).unwrap(),
        };
        let (min, max) = table_codec.tuple_bound();

        let vec = set
            .range::<Vec<u8>, (Bound<&Vec<u8>>, Bound<&Vec<u8>>)>((Bound::Included(&min), Bound::Included(&max)))
            .collect_vec();

        assert_eq!(String::from_utf8(vec[0].clone()).unwrap(), "Data_T1_0000000000");
        assert_eq!(String::from_utf8(vec[1].clone()).unwrap(), "Data_T1_0000000001");
        assert_eq!(String::from_utf8(vec[2].clone()).unwrap(), "Data_T1_0000000002");
    }
}