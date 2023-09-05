use std::sync::Arc;
use bytes::Bytes;
use crate::catalog::{ColumnRef, TableCatalog, TableName};
use crate::types::ColumnId;
use crate::types::tuple::Tuple;

#[derive(Clone)]
pub struct TableCodec {
    table: TableCatalog
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

    /// Key: Catalog_TableName_ColumnName_ColumnId
    /// Value: TableCatalog
    pub fn encode_column(&self, id: ColumnId) -> Option<(Bytes, Bytes)> {
        self.table
            .get_column_by_id(&id)
            .and_then(|col| {
                bincode::serialize(&col).ok()
                    .map(|bytes| {
                        let key = format!(
                            "Catalog_{}_{}_{:0width$}",
                            self.table.name,
                            col.name,
                            col.id,
                            width = std::mem::size_of::<usize>() * 2 - 6
                        );

                        (Bytes::from(key.into_bytes()), Bytes::from(bytes))
                    })
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
    use std::sync::Arc;
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
        let (table_catalog, codec) = build_table_codec();
        let col = table_catalog.all_columns()[0].clone();
        let (key, bytes) = codec.encode_column(col.id).unwrap();

        assert_eq!(
            String::from_utf8(key.to_vec()).ok().unwrap(),
            format!(
                "Catalog_{}_{}_{:0width$}",
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
}