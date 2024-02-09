use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::expression::simplify::ConstantBinary;
use crate::optimizer::core::cm_sketch::CountMinSketch;
use crate::optimizer::core::histogram::Histogram;
use crate::storage::Transaction;
use crate::types::value::DataValue;
use crate::types::{ColumnId, LogicalType};
use kip_db::kernel::utils::lru_cache::ShardingLruCache;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;

pub struct ColumnMetaLoader<'a, T: Transaction> {
    cache: &'a ShardingLruCache<TableName, Vec<ColumnMeta>>,
    tx: &'a T,
}

impl<'a, T: Transaction> ColumnMetaLoader<'a, T> {
    pub fn new(
        tx: &'a T,
        cache: &'a ShardingLruCache<TableName, Vec<ColumnMeta>>,
    ) -> ColumnMetaLoader<'a, T> {
        ColumnMetaLoader { cache, tx }
    }

    pub fn load(&self, table_name: TableName) -> Result<&Vec<ColumnMeta>, DatabaseError> {
        let option = self.cache.get(&table_name);

        if let Some(column_metas) = option {
            Ok(column_metas)
        } else {
            let paths = self.tx.column_meta_paths(&table_name)?;
            let mut column_metas = Vec::with_capacity(paths.len());

            for path in paths {
                column_metas.push(ColumnMeta::from_file(path)?);
            }

            Ok(self.cache.get_or_insert(table_name, |_| Ok(column_metas))?)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnMeta {
    column_id: ColumnId,
    data_type: LogicalType,
    histogram: Histogram,
    cm_sketch: CountMinSketch<DataValue>,
}

impl ColumnMeta {
    pub fn new(histogram: Histogram, cm_sketch: CountMinSketch<DataValue>) -> Self {
        ColumnMeta {
            column_id: histogram.column_id(),
            data_type: histogram.data_type(),
            histogram,
            cm_sketch,
        }
    }

    pub fn column_id(&self) -> ColumnId {
        self.column_id
    }
    pub fn data_type(&self) -> LogicalType {
        self.data_type
    }

    pub fn histogram(&self) -> &Histogram {
        &self.histogram
    }

    /// Tips:
    /// - binaries must be used `ConstantBinary::scope_aggregation` and `ConstantBinary::rearrange`
    pub fn collect_count(&self, binaries: &[ConstantBinary]) -> usize {
        let mut count = 0;

        count += self.histogram.collect_count(binaries, &self.cm_sketch);
        count
    }

    pub fn to_file(&self, path: impl AsRef<Path>) -> Result<(), DatabaseError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        file.write_all(&bincode::serialize(self)?)?;
        file.flush()?;

        Ok(())
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;

        let mut bytes = Vec::new();
        let _ = file.read_to_end(&mut bytes)?;

        Ok(bincode::deserialize(&bytes)?)
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnSummary};
    use crate::errors::DatabaseError;
    use crate::optimizer::core::column_meta::ColumnMeta;
    use crate::optimizer::core::histogram::HistogramBuilder;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn int32_column() -> ColumnCatalog {
        ColumnCatalog {
            summary: ColumnSummary {
                id: Some(1),
                name: "c1".to_string(),
                table_name: None,
            },
            nullable: false,
            desc: ColumnDesc {
                column_datatype: LogicalType::UInteger,
                is_primary: false,
                is_unique: false,
                default: None,
            },
        }
    }

    #[test]
    fn test_to_file_and_from_file() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let column = int32_column();

        let mut builder = HistogramBuilder::new(&column, Some(15))?;

        builder.append(&Arc::new(DataValue::Int32(Some(14))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(13))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(12))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(11))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(10))))?;

        builder.append(&Arc::new(DataValue::Int32(Some(4))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(3))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(2))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(1))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(0))))?;

        builder.append(&Arc::new(DataValue::Int32(Some(9))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(8))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(7))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(6))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(5))))?;

        builder.append(&Arc::new(DataValue::Null))?;
        builder.append(&Arc::new(DataValue::Int32(None)))?;

        let (histogram, sketch) = builder.build(4)?;
        let path = temp_dir.path().join("meta");

        ColumnMeta::new(histogram.clone(), sketch.clone()).to_file(path.clone())?;
        let column_meta = ColumnMeta::from_file(path)?;

        assert_eq!(histogram, column_meta.histogram);
        assert_eq!(
            sketch.estimate(&DataValue::Null),
            column_meta.cm_sketch.estimate(&DataValue::Null)
        );

        Ok(())
    }
}
