use std::sync::Arc;
use bytes::Bytes;
use itertools::Itertools;
use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, TableCatalog};
use crate::types::{ColumnId, TableId};
use crate::types::tuple::Tuple;

struct TableCodec {
    table: TableCatalog
}

impl TableCodec {

    /// Key: Data_TableName_TableId_RowID(Sorted)
    /// Value: Tuple
    fn encode_tuple(&self, tuple: &Tuple) -> (String, Bytes) {
        let key = format!(
            "Data_{}_{}_{:0width$}",
            self.table.name,
            self.table.id,
            tuple.id.unwrap(),
            width = std::mem::size_of::<usize>() * 2 - 6
        );
        (key, Bytes::from(tuple.serialize_to()))
    }

    fn decode_tuple(&self, key: String, bytes: &[u8]) -> Option<Tuple> {
        key.split("_")
            .nth(3)
            .and_then(|last| last.parse::<usize>().ok()
                .map(|row_id| {
                    Tuple::deserialize_from(
                        Some(row_id),
                        self.table.all_columns(),
                        bytes
                    )
                }))
    }

    /// Key: Catalog_TableName_TableId_ColumnName_ColumnId_true(nullable)
    /// Value: ColumnDesc
    fn encode_column(&self, id: ColumnId) -> Option<(String, Bytes)> {
        self.table
            .get_column_by_id(&id)
            .and_then(|col| {
                bincode::serialize(&col.desc).ok()
                    .map(|bytes| {
                        let key = format!(
                            "Catalog_{}_{}_{}_{:0width$}_{}",
                            self.table.name,
                            self.table.id,
                            col.name,
                            col.id,
                            col.nullable,
                            width = std::mem::size_of::<usize>() * 2 - 6
                        );

                        (key, Bytes::from(bytes))
                    })
            })
    }

    fn decode_column(key: String, bytes: &[u8]) -> Option<(ColumnRef, String)> {
        let fields = key.split("_").collect_vec();

        bincode::deserialize::<ColumnDesc>(bytes).ok()
            .and_then(|desc| {
                let column = ColumnCatalog {
                    id: fields[4].parse::<TableId>().ok()?,
                    name: fields[3].to_string(),
                    table_id: Some(fields[2].parse::<TableId>().ok()?),
                    nullable: fields[5].parse::<bool>().ok()?,
                    desc,
                };

                Some((Arc::new(column), fields[1].to_string()))
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
        let table_catalog = TableCatalog::new("t1".to_string(), columns).unwrap();
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
            key,
            format!(
                "Data_{}_{}_{:0width$}",
                table_catalog.name,
                table_catalog.id,
                tuple.id.unwrap(),
                width = std::mem::size_of::<usize>() * 2 - 6
            )
        );
        assert_eq!(codec.decode_tuple(key, &bytes).unwrap(), tuple)
    }

    #[test]
    fn test_table_codec_column() {
        let (table_catalog, codec) = build_table_codec();
        let col = table_catalog.all_columns()[0].clone();
        let (key, bytes) = codec.encode_column(col.id).unwrap();

        assert_eq!(
            key,
            format!(
                "Catalog_{}_{}_{}_{:0width$}_{}",
                table_catalog.name,
                table_catalog.id,
                col.name,
                col.id,
                col.nullable,
                width = std::mem::size_of::<usize>() * 2 - 6
            )
        );

        let (decode_col, table_name) = TableCodec::decode_column(key, &bytes).unwrap();

        assert_eq!(decode_col, col);
        assert_eq!(table_name, table_catalog.name);
    }
}