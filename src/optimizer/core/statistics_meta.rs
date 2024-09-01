use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::expression::range_detacher::Range;
use crate::optimizer::core::cm_sketch::CountMinSketch;
use crate::optimizer::core::histogram::Histogram;
use crate::storage::{StatisticsMetaCache, Transaction};
use crate::types::index::IndexId;
use crate::types::value::DataValue;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::slice;

pub struct StatisticMetaLoader<'a, T: Transaction> {
    cache: &'a StatisticsMetaCache,
    tx: &'a T,
}

impl<'a, T: Transaction> StatisticMetaLoader<'a, T> {
    pub fn new(tx: &'a T, cache: &'a StatisticsMetaCache) -> StatisticMetaLoader<'a, T> {
        StatisticMetaLoader { cache, tx }
    }

    pub fn load(
        &self,
        table_name: &TableName,
        index_id: IndexId,
    ) -> Result<Option<&StatisticsMeta>, DatabaseError> {
        let key = (table_name.clone(), index_id);
        let option = self.cache.get(&key);

        if let Some(statistics_meta) = option {
            return Ok(Some(statistics_meta));
        }
        if let Some(path) = self.tx.table_meta_path(table_name.as_str(), index_id)? {
            Ok(Some(
                self.cache
                    .get_or_insert(key, |_| StatisticsMeta::from_file(path))?,
            ))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatisticsMeta {
    index_id: IndexId,
    histogram: Histogram,
    cm_sketch: CountMinSketch<DataValue>,
}

impl StatisticsMeta {
    pub fn new(histogram: Histogram, cm_sketch: CountMinSketch<DataValue>) -> Self {
        StatisticsMeta {
            index_id: histogram.index_id(),
            histogram,
            cm_sketch,
        }
    }
    pub fn index_id(&self) -> IndexId {
        self.index_id
    }
    pub fn histogram(&self) -> &Histogram {
        &self.histogram
    }

    pub fn collect_count(&self, range: &Range) -> Result<usize, DatabaseError> {
        let mut count = 0;

        let ranges = if let Range::SortedRanges(ranges) = range {
            ranges.as_slice()
        } else {
            slice::from_ref(range)
        };
        count += self.histogram.collect_count(ranges, &self.cm_sketch)?;
        Ok(count)
    }

    pub fn to_file(&self, path: impl AsRef<Path>) -> Result<(), DatabaseError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(false)
            .open(path)?;
        bincode::serialize_into(&mut file, self)?;
        file.flush()?;

        Ok(())
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(false)
            .open(path)?;
        Ok(bincode::deserialize_from(file)?)
    }
}

#[cfg(test)]
mod tests {
    use crate::errors::DatabaseError;
    use crate::optimizer::core::histogram::HistogramBuilder;
    use crate::optimizer::core::statistics_meta::StatisticsMeta;
    use crate::types::index::{IndexMeta, IndexType};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_to_file_and_from_file() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let index = IndexMeta {
            id: 0,
            column_ids: vec![0],
            table_name: Arc::new("t1".to_string()),
            pk_ty: LogicalType::Integer,
            name: "pk_c1".to_string(),
            ty: IndexType::PrimaryKey,
        };

        let mut builder = HistogramBuilder::new(&index, Some(15))?;

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

        StatisticsMeta::new(histogram.clone(), sketch.clone()).to_file(path.clone())?;
        let statistics_meta = StatisticsMeta::from_file(path)?;

        debug_assert_eq!(histogram, statistics_meta.histogram);
        debug_assert_eq!(
            sketch.estimate(&DataValue::Null),
            statistics_meta.cm_sketch.estimate(&DataValue::Null)
        );

        Ok(())
    }
}
