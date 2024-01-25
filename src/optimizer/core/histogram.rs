use crate::catalog::{ColumnCatalog, TableName};
use crate::execution::volcano::dql::sort::radix_sort;
use crate::expression::simplify::ConstantBinary;
use crate::optimizer::utils::hyper_log_log::HyperLogLog;
use crate::optimizer::OptimizerError;
use crate::storage::Transaction;
use crate::types::value::{DataValue, ValueRef};
use crate::types::{ColumnId, LogicalType};
use kip_db::kernel::utils::lru_cache::ShardingLruCache;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::Bound;
use std::fs::OpenOptions;
use std::hash::RandomState;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::{cmp, mem};

const DEFAULT_HYPERLOGLOG_BUCKET_SIZE: u8 = 14;

pub struct HistogramBuilder {
    column_id: ColumnId,
    data_type: LogicalType,

    hyperloglog: HyperLogLog,
    null_count: usize,
    values: Vec<((usize, ValueRef), Vec<u8>)>,

    value_index: usize,
}

pub struct HistogramLoader<'a, T: Transaction> {
    cache: ShardingLruCache<TableName, Vec<Histogram>>,
    tx: &'a T,
}

// Equal depth histogram
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Histogram {
    column_id: ColumnId,
    data_type: LogicalType,

    hyperloglog: HyperLogLog,
    null_count: usize,
    values_len: usize,

    buckets: Vec<Bucket>,
    // Correlation is the statistical correlation between physical row ordering and logical ordering of
    // the column values
    correlation: f64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Bucket {
    lower: ValueRef,
    upper: ValueRef,
    count: u64,
    // repeat: u64,
}

impl HistogramBuilder {
    pub fn new(column: &ColumnCatalog, capacity: Option<usize>) -> Result<Self, OptimizerError> {
        let hyperloglog = HyperLogLog::new(DEFAULT_HYPERLOGLOG_BUCKET_SIZE).unwrap();

        Ok(Self {
            column_id: column.id().ok_or(OptimizerError::OwnerLessColumn)?,
            data_type: *column.datatype(),
            hyperloglog,
            null_count: 0,
            values: capacity
                .map(Vec::with_capacity)
                .unwrap_or_else(|| Vec::new()),
            value_index: 0,
        })
    }

    pub fn append(&mut self, value: &ValueRef) -> Result<(), OptimizerError> {
        if value.is_null() {
            self.null_count += 1;
        } else {
            let mut bytes = Vec::new();

            value.memcomparable_encode(&mut bytes)?;
            self.values.push(((self.value_index, value.clone()), bytes));
            self.hyperloglog.insert(value);
        }

        self.value_index += 1;

        Ok(())
    }

    pub fn build(self, number_of_buckets: usize) -> Result<Histogram, OptimizerError> {
        if number_of_buckets > self.values.len() {
            return Err(OptimizerError::TooManyBuckets);
        }

        let HistogramBuilder {
            column_id,
            data_type,
            hyperloglog,
            null_count,
            values,
            ..
        } = self;
        let mut buckets = Vec::with_capacity(number_of_buckets);
        let values_len = values.len();
        let bucket_len = if values_len % number_of_buckets == 0 {
            values_len / number_of_buckets
        } else {
            (values_len + number_of_buckets) / number_of_buckets
        };
        let sorted_values = radix_sort(values);

        for i in 0..number_of_buckets {
            let mut bucket = Bucket::empty(&data_type);
            let j = (i + 1) * bucket_len;

            bucket.upper = sorted_values[cmp::min(j, values_len) - 1].1.clone();
            buckets.push(bucket);
        }
        let mut corr_xy_sum = 0.0;

        for (i, (ordinal, value)) in sorted_values.into_iter().enumerate() {
            let bucket = &mut buckets[i / bucket_len];

            if bucket.lower.is_null() {
                bucket.lower = value;
            }
            bucket.count += 1;

            corr_xy_sum += i as f64 * ordinal as f64;
        }

        Ok(Histogram {
            column_id,
            data_type,
            hyperloglog,
            null_count,
            values_len,
            buckets,
            correlation: Self::calc_correlation(corr_xy_sum, values_len),
        })
    }

    // https://github.com/pingcap/tidb/blob/6957170f1147e96958e63db48148445a7670328e/pkg/statistics/builder.go#L210
    fn calc_correlation(corr_xy_sum: f64, values_len: usize) -> f64 {
        if values_len == 1 {
            return 1.0;
        }
        let item_count = values_len as f64;
        let corr_x_sum = (item_count - 1.0) * item_count / 2.0;
        let corr_x2_sum = (item_count - 1.0) * item_count * (2.0 * item_count - 1.0) / 6.0;
        (item_count * corr_xy_sum - corr_x_sum * corr_x_sum)
            / (item_count * corr_x2_sum - corr_x_sum * corr_x_sum)
    }
}

impl<'a, T: Transaction> HistogramLoader<'a, T> {
    pub fn new(tx: &'a T) -> Result<HistogramLoader<T>, OptimizerError> {
        let cache = ShardingLruCache::new(128, 16, RandomState::new())?;

        Ok(HistogramLoader { cache, tx })
    }

    pub fn load(&self, table_name: TableName) -> Result<&Vec<Histogram>, OptimizerError> {
        let option = self.cache.get(&table_name);

        return if let Some(histograms) = option {
            Ok(histograms)
        } else {
            let paths = self.tx.histogram_paths(&table_name)?;
            let mut histograms = Vec::with_capacity(paths.len());

            for path in paths {
                histograms.push(Histogram::from_file(path)?);
            }

            Ok(self.cache.get_or_insert(table_name, |_| Ok(histograms))?)
        };
    }
}

fn is_under(value: &ValueRef, target: &Bound<ValueRef>, is_min: bool) -> bool {
    let _is_under = |value: &ValueRef, target: &ValueRef, is_min: bool| {
        value
            .partial_cmp(target)
            .map(|order| {
                if is_min {
                    Ordering::is_lt(order)
                } else {
                    Ordering::is_le(order)
                }
            })
            .unwrap()
    };

    match target {
        Bound::Included(target) => _is_under(value, target, is_min),
        Bound::Excluded(target) => _is_under(value, target, !is_min),
        Bound::Unbounded => !is_min,
    }
}

fn is_above(value: &ValueRef, target: &Bound<ValueRef>, is_min: bool) -> bool {
    let _is_above = |value: &ValueRef, target: &ValueRef, is_min: bool| {
        value
            .partial_cmp(target)
            .map(|order| {
                if is_min {
                    Ordering::is_ge(order)
                } else {
                    Ordering::is_gt(order)
                }
            })
            .unwrap()
    };

    match target {
        Bound::Included(target) => _is_above(value, target, is_min),
        Bound::Excluded(target) => _is_above(value, target, !is_min),
        Bound::Unbounded => is_min,
    }
}

impl Histogram {
    pub fn column_id(&self) -> ColumnId {
        self.column_id
    }
    pub fn data_type(&self) -> LogicalType {
        self.data_type
    }

    pub fn values_len(&self) -> usize {
        self.values_len
    }

    /// Tips: binaries must be used `ConstantBinary::scope_aggregation` and `ConstantBinary::rearrange`
    pub fn collect_count(&self, binaries: &[ConstantBinary]) -> usize {
        if self.buckets.is_empty() || binaries.is_empty() {
            return 0;
        }

        let mut binary_i = 0;
        let mut bucket_i = 0;
        let mut bucket_idxs = Vec::new();

        while bucket_i < self.buckets.len() && binary_i < binaries.len() {
            self._collect_count(&binaries, &mut binary_i, &mut bucket_i, &mut bucket_idxs);
        }

        bucket_idxs
            .iter()
            .map(|idx| self.buckets[*idx].count as usize)
            .sum()
    }

    fn _collect_count(
        &self,
        binaries: &[ConstantBinary],
        binary_i: &mut usize,
        bucket_i: &mut usize,
        bucket_idxs: &mut Vec<usize>,
    ) {
        match &binaries[*binary_i] {
            ConstantBinary::Scope { min, max } => {
                let bucket = &self.buckets[*bucket_i];

                let is_eq = |value: &ValueRef, target: &Bound<ValueRef>| match target {
                    Bound::Included(target) => target.eq(value),
                    _ => false,
                };

                let is_meet = (is_under(&bucket.lower, min, true)
                    && is_above(&bucket.upper, min, true))
                    || (is_under(&bucket.lower, max, false) && is_above(&bucket.upper, max, false))
                    || (is_under(&bucket.lower, min, true) && is_above(&bucket.upper, max, false))
                    || (is_above(&bucket.lower, min, true) && is_under(&bucket.upper, max, false))
                    || is_eq(&bucket.lower, min)
                    || is_eq(&bucket.lower, max)
                    || is_eq(&bucket.upper, min)
                    || is_eq(&bucket.upper, max);

                if is_meet {
                    bucket_idxs.push(mem::replace(bucket_i, *bucket_i + 1));
                } else if is_under(&bucket.lower, min, true) {
                    *bucket_i += 1
                } else {
                    *binary_i += 1;
                }
            }
            ConstantBinary::Eq(value) => {
                let bucket = &self.buckets[*bucket_i];

                if bucket
                    .lower
                    .partial_cmp(value)
                    .map(Ordering::is_le)
                    .unwrap()
                    && bucket
                        .upper
                        .partial_cmp(value)
                        .map(Ordering::is_ge)
                        .unwrap()
                {
                    bucket_idxs.push(mem::replace(bucket_i, *bucket_i + 1));
                }
                *binary_i += 1;
            }
            ConstantBinary::NotEq(_) => (),
            ConstantBinary::And(inner_binaries) | ConstantBinary::Or(inner_binaries) => {
                self._collect_count(inner_binaries, binary_i, bucket_i, bucket_idxs)
            }
        }
    }

    pub fn to_file(&self, path: impl AsRef<Path>) -> Result<(), OptimizerError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        let _ = file.write_all(&bincode::serialize(self)?)?;
        file.flush()?;

        Ok(())
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, OptimizerError> {
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

impl Bucket {
    fn empty(data_type: &LogicalType) -> Self {
        let empty_value = Arc::new(DataValue::none(data_type));

        Bucket {
            lower: empty_value.clone(),
            upper: empty_value,
            count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnSummary};
    use crate::expression::simplify::ConstantBinary;
    use crate::optimizer::core::histogram::{Bucket, Histogram, HistogramBuilder};
    use crate::optimizer::OptimizerError;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::ops::Bound;
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
            ref_expr: None,
        }
    }

    #[test]
    fn test_sort_tuples_on_histogram() -> Result<(), OptimizerError> {
        let column = int32_column();

        let mut builder = HistogramBuilder::new(&column, Some(15))?;

        builder.append(&Arc::new(DataValue::Int32(Some(0))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(1))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(2))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(3))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(4))))?;

        builder.append(&Arc::new(DataValue::Int32(Some(5))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(6))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(7))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(8))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(9))))?;

        builder.append(&Arc::new(DataValue::Int32(Some(10))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(11))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(12))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(13))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(14))))?;

        builder.append(&Arc::new(DataValue::Null))?;
        builder.append(&Arc::new(DataValue::Int32(None)))?;

        // assert!(matches!(builder.build(10), Err(OptimizerError::TooManyBuckets)));

        let histogram = builder.build(5)?;

        assert_eq!(histogram.correlation, 1.0);
        assert_eq!(histogram.null_count, 2);
        assert_eq!(histogram.buckets.len(), 5);
        assert_eq!(
            histogram.buckets,
            vec![
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(0))),
                    upper: Arc::new(DataValue::Int32(Some(2))),
                    count: 3,
                },
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(3))),
                    upper: Arc::new(DataValue::Int32(Some(5))),
                    count: 3,
                },
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(6))),
                    upper: Arc::new(DataValue::Int32(Some(8))),
                    count: 3,
                },
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(9))),
                    upper: Arc::new(DataValue::Int32(Some(11))),
                    count: 3,
                },
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(12))),
                    upper: Arc::new(DataValue::Int32(Some(14))),
                    count: 3,
                },
            ]
        );

        Ok(())
    }

    #[test]
    fn test_rev_sort_tuples_on_histogram() -> Result<(), OptimizerError> {
        let column = int32_column();

        let mut builder = HistogramBuilder::new(&column, Some(15))?;

        builder.append(&Arc::new(DataValue::Int32(Some(14))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(13))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(12))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(11))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(10))))?;

        builder.append(&Arc::new(DataValue::Int32(Some(9))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(8))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(7))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(6))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(5))))?;

        builder.append(&Arc::new(DataValue::Int32(Some(4))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(3))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(2))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(1))))?;
        builder.append(&Arc::new(DataValue::Int32(Some(0))))?;

        builder.append(&Arc::new(DataValue::Null))?;
        builder.append(&Arc::new(DataValue::Int32(None)))?;

        let histogram = builder.build(5)?;

        assert_eq!(histogram.correlation, -1.0);
        assert_eq!(histogram.null_count, 2);
        assert_eq!(histogram.buckets.len(), 5);
        assert_eq!(
            histogram.buckets,
            vec![
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(0))),
                    upper: Arc::new(DataValue::Int32(Some(2))),
                    count: 3,
                },
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(3))),
                    upper: Arc::new(DataValue::Int32(Some(5))),
                    count: 3,
                },
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(6))),
                    upper: Arc::new(DataValue::Int32(Some(8))),
                    count: 3,
                },
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(9))),
                    upper: Arc::new(DataValue::Int32(Some(11))),
                    count: 3,
                },
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(12))),
                    upper: Arc::new(DataValue::Int32(Some(14))),
                    count: 3,
                },
            ]
        );

        Ok(())
    }

    #[test]
    fn test_non_average_on_histogram() -> Result<(), OptimizerError> {
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

        let histogram = builder.build(4)?;

        assert!(histogram.correlation < 0.0);
        assert_eq!(histogram.null_count, 2);
        assert_eq!(histogram.buckets.len(), 4);
        assert_eq!(
            histogram.buckets,
            vec![
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(0))),
                    upper: Arc::new(DataValue::Int32(Some(3))),
                    count: 4,
                },
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(4))),
                    upper: Arc::new(DataValue::Int32(Some(7))),
                    count: 4,
                },
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(8))),
                    upper: Arc::new(DataValue::Int32(Some(11))),
                    count: 4,
                },
                Bucket {
                    lower: Arc::new(DataValue::Int32(Some(12))),
                    upper: Arc::new(DataValue::Int32(Some(14))),
                    count: 3,
                },
            ]
        );

        Ok(())
    }

    #[test]
    fn test_collect_count() -> Result<(), OptimizerError> {
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

        let histogram = builder.build(4)?;

        let count_1 = histogram.collect_count(&vec![
            ConstantBinary::Eq(Arc::new(DataValue::Int32(Some(2)))),
            ConstantBinary::Scope {
                min: Bound::Included(Arc::new(DataValue::Int32(Some(4)))),
                max: Bound::Excluded(Arc::new(DataValue::Int32(Some(12)))),
            },
        ]);

        assert_eq!(count_1, 12);

        let count_2 = histogram.collect_count(&vec![ConstantBinary::Scope {
            min: Bound::Included(Arc::new(DataValue::Int32(Some(4)))),
            max: Bound::Unbounded,
        }]);

        assert_eq!(count_2, 11);

        let count_3 = histogram.collect_count(&vec![ConstantBinary::Scope {
            min: Bound::Excluded(Arc::new(DataValue::Int32(Some(7)))),
            max: Bound::Unbounded,
        }]);

        assert_eq!(count_3, 7);

        let count_4 = histogram.collect_count(&vec![ConstantBinary::Scope {
            min: Bound::Unbounded,
            max: Bound::Included(Arc::new(DataValue::Int32(Some(11)))),
        }]);

        assert_eq!(count_4, 12);

        let count_5 = histogram.collect_count(&vec![ConstantBinary::Scope {
            min: Bound::Unbounded,
            max: Bound::Excluded(Arc::new(DataValue::Int32(Some(8)))),
        }]);

        assert_eq!(count_5, 8);

        Ok(())
    }

    #[test]
    fn test_to_file_and_from_file() -> Result<(), OptimizerError> {
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

        let histogram = builder.build(4)?;
        let path = temp_dir.path().join("histogram");

        histogram.to_file(path.clone())?;
        let histogram_from_file = Histogram::from_file(path)?;

        assert_eq!(histogram, histogram_from_file);

        Ok(())
    }
}
