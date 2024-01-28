use crate::catalog::ColumnCatalog;
use crate::execution::volcano::dql::sort::radix_sort;
use crate::expression::simplify::ConstantBinary;
use crate::optimizer::core::cm_sketch::CountMinSketch;
use crate::optimizer::OptimizerError;
use crate::types::value::{DataValue, ValueRef};
use crate::types::{ColumnId, LogicalType};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::Bound;
use std::sync::Arc;
use std::{cmp, mem};

pub struct HistogramBuilder {
    column_id: ColumnId,
    data_type: LogicalType,

    null_count: usize,
    values: Vec<((usize, ValueRef), Vec<u8>)>,

    value_index: usize,
}

// Equal depth histogram
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Histogram {
    column_id: ColumnId,
    data_type: LogicalType,

    number_of_distinct_value: usize,
    null_count: usize,
    values_len: usize,

    buckets: Vec<Bucket>,
    // TODO: How to use?
    // Correlation is the statistical correlation between physical row ordering and logical ordering of
    // the column values
    correlation: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Bucket {
    lower: ValueRef,
    upper: ValueRef,
    count: u64,
    // repeat: u64,
}

impl HistogramBuilder {
    pub fn new(column: &ColumnCatalog, capacity: Option<usize>) -> Result<Self, OptimizerError> {
        Ok(Self {
            column_id: column.id().ok_or(OptimizerError::OwnerLessColumn)?,
            data_type: *column.datatype(),
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
        }

        self.value_index += 1;

        Ok(())
    }

    pub fn build(
        self,
        number_of_buckets: usize,
    ) -> Result<(Histogram, CountMinSketch<DataValue>), OptimizerError> {
        if number_of_buckets > self.values.len() {
            return Err(OptimizerError::TooManyBuckets);
        }

        let tolerance = if self.values.len() > 10_000 {
            (self.values.len() / 100) as f64
        } else {
            1.0
        };
        let mut sketch = CountMinSketch::new(self.values.len(), 0.95, tolerance);
        let HistogramBuilder {
            column_id,
            data_type,
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
        let mut number_of_distinct_value = 0;
        let mut last_value: Option<ValueRef> = None;

        for (i, (ordinal, value)) in sorted_values.into_iter().enumerate() {
            sketch.increment(value.as_ref());

            if let None | Some(true) = last_value.as_ref().map(|last_value| last_value != &value) {
                last_value = Some(value.clone());
                number_of_distinct_value += 1;
            }

            let bucket = &mut buckets[i / bucket_len];

            if bucket.lower.is_null() {
                bucket.lower = value;
            }
            bucket.count += 1;

            corr_xy_sum += i as f64 * ordinal as f64;
        }
        sketch.add(&DataValue::Null, self.null_count);

        Ok((
            Histogram {
                column_id,
                data_type,
                number_of_distinct_value,
                null_count,
                values_len,
                buckets,
                correlation: Self::calc_correlation(corr_xy_sum, values_len),
            },
            sketch,
        ))
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
    pub fn collect_count(
        &self,
        binaries: &[ConstantBinary],
        sketch: &CountMinSketch<DataValue>,
    ) -> usize {
        if self.buckets.is_empty() || binaries.is_empty() {
            return 0;
        }

        let mut count = 0;
        let mut binary_i = 0;
        let mut bucket_i = 0;
        let mut bucket_idxs = Vec::new();

        while bucket_i < self.buckets.len() && binary_i < binaries.len() {
            self._collect_count(
                &binaries,
                &mut binary_i,
                &mut bucket_i,
                &mut bucket_idxs,
                &mut count,
                sketch,
            );
        }

        bucket_idxs
            .iter()
            .map(|idx| self.buckets[*idx].count as usize)
            .sum::<usize>()
            + count
    }

    fn _collect_count(
        &self,
        binaries: &[ConstantBinary],
        binary_i: &mut usize,
        bucket_i: &mut usize,
        bucket_idxs: &mut Vec<usize>,
        count: &mut usize,
        sketch: &CountMinSketch<DataValue>,
    ) {
        let float_value = |value: &DataValue, prefix_len: usize| {
            match value.logical_type() {
                LogicalType::Varchar(_) => match value {
                    DataValue::Utf8(value) => value.as_ref().and_then(|string| {
                        if prefix_len > string.len() {
                            return Some(0.0);
                        }

                        let mut val = 0u64;
                        for (i, char) in string
                            .get(prefix_len..prefix_len + 8)
                            .unwrap()
                            .chars()
                            .enumerate()
                        {
                            if string.len() - prefix_len > i {
                                val += (val << 8) + char as u64;
                            } else {
                                val += val << 8;
                            }
                        }

                        Some(val as f64)
                    }),
                    _ => unreachable!(),
                },
                LogicalType::Date | LogicalType::DateTime => match value {
                    DataValue::Date32(value) => DataValue::Int32(*value)
                        .cast(&LogicalType::Double)
                        .unwrap()
                        .double(),
                    DataValue::Date64(value) => DataValue::Int64(*value)
                        .cast(&LogicalType::Double)
                        .unwrap()
                        .double(),
                    _ => unreachable!(),
                },

                LogicalType::Invalid
                | LogicalType::SqlNull
                | LogicalType::Boolean
                | LogicalType::Tinyint
                | LogicalType::UTinyint
                | LogicalType::Smallint
                | LogicalType::USmallint
                | LogicalType::Integer
                | LogicalType::UInteger
                | LogicalType::Bigint
                | LogicalType::UBigint
                | LogicalType::Float
                | LogicalType::Double
                | LogicalType::Decimal(_, _) => {
                    value.clone().cast(&LogicalType::Double).unwrap().double()
                }
            }
            .unwrap_or(0.0)
        };
        let calc_fraction = |start: &DataValue, end: &DataValue, value: &DataValue| {
            let prefix_len = start.common_prefix_length(end).unwrap_or(0);
            (float_value(value, prefix_len) - float_value(start, prefix_len))
                / (float_value(end, prefix_len) - float_value(start, prefix_len))
        };

        let distinct_1 = OrderedFloat(1.0 / self.number_of_distinct_value as f64);

        match &binaries[*binary_i] {
            ConstantBinary::Scope { min, max } => {
                let bucket = &self.buckets[*bucket_i];
                let mut temp_count = 0;

                let is_eq = |value: &ValueRef, target: &Bound<ValueRef>| match target {
                    Bound::Included(target) => target.eq(value),
                    _ => false,
                };

                if (is_above(&bucket.lower, min, true) || is_eq(&bucket.lower, min))
                    && (is_under(&bucket.upper, max, false) || is_eq(&bucket.upper, max))
                {
                    bucket_idxs.push(mem::replace(bucket_i, *bucket_i + 1));
                } else if is_above(&bucket.lower, max, false) {
                    *binary_i += 1;
                } else if is_under(&bucket.upper, min, true) {
                    *bucket_i += 1;
                } else if is_above(&bucket.lower, min, true) {
                    let (temp_ratio, option) = match max {
                        Bound::Included(val) => {
                            (calc_fraction(&bucket.lower, &bucket.upper, val), None)
                        }
                        Bound::Excluded(val) => (
                            calc_fraction(&bucket.lower, &bucket.upper, val),
                            Some(sketch.estimate(val)),
                        ),
                        Bound::Unbounded => unreachable!(),
                    };
                    let ratio = *distinct_1.max(OrderedFloat(temp_ratio).min(OrderedFloat(1.0)));
                    temp_count += (bucket.count as f64 * ratio).ceil() as usize;
                    if let Some(count) = option {
                        temp_count = temp_count.saturating_sub(count);
                    }
                    *bucket_i += 1;
                } else if is_under(&bucket.upper, max, false) {
                    let (temp_ratio, option) = match min {
                        Bound::Included(val) => {
                            (calc_fraction(&bucket.lower, &bucket.upper, val), None)
                        }
                        Bound::Excluded(val) => (
                            calc_fraction(&bucket.lower, &bucket.upper, val),
                            Some(sketch.estimate(val)),
                        ),
                        Bound::Unbounded => unreachable!(),
                    };
                    let ratio = *distinct_1.max(OrderedFloat(temp_ratio).min(OrderedFloat(1.0)));
                    temp_count += (bucket.count as f64 * (1.0 - ratio)).ceil() as usize;
                    if let Some(count) = option {
                        temp_count = temp_count.saturating_sub(count);
                    }
                    *bucket_i += 1;
                } else {
                    let (temp_ratio_max, option_max) = match max {
                        Bound::Included(val) => {
                            (calc_fraction(&bucket.lower, &bucket.upper, val), None)
                        }
                        Bound::Excluded(val) => (
                            calc_fraction(&bucket.lower, &bucket.upper, val),
                            Some(sketch.estimate(val)),
                        ),
                        Bound::Unbounded => unreachable!(),
                    };
                    let (temp_ratio_min, option_min) = match min {
                        Bound::Included(val) => {
                            (calc_fraction(&bucket.lower, &bucket.upper, val), None)
                        }
                        Bound::Excluded(val) => (
                            calc_fraction(&bucket.lower, &bucket.upper, val),
                            Some(sketch.estimate(val)),
                        ),
                        Bound::Unbounded => unreachable!(),
                    };
                    let ratio = *distinct_1
                        .max(OrderedFloat(temp_ratio_max - temp_ratio_min).min(OrderedFloat(1.0)));
                    temp_count += (bucket.count as f64 * ratio).ceil() as usize;
                    if let Some(count) = option_max {
                        temp_count = temp_count.saturating_sub(count);
                    }
                    if let Some(count) = option_min {
                        temp_count = temp_count.saturating_sub(count);
                    }
                    *binary_i += 1;
                }
                *count += cmp::max(temp_count, 0);
            }
            ConstantBinary::Eq(value) => {
                *count += sketch.estimate(value);
                *binary_i += 1
            }
            ConstantBinary::NotEq(_) => (),
            ConstantBinary::And(inner_binaries) | ConstantBinary::Or(inner_binaries) => self
                ._collect_count(
                    inner_binaries,
                    binary_i,
                    bucket_i,
                    bucket_idxs,
                    count,
                    sketch,
                ),
        }
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
    use crate::optimizer::core::histogram::{Bucket, HistogramBuilder};
    use crate::optimizer::OptimizerError;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::ops::Bound;
    use std::sync::Arc;

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

        let (histogram, _) = builder.build(5)?;

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

        let (histogram, _) = builder.build(5)?;

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

        let (histogram, _) = builder.build(4)?;

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

        let (histogram, sketch) = builder.build(4)?;

        let count_1 = histogram.collect_count(
            &vec![
                ConstantBinary::Eq(Arc::new(DataValue::Int32(Some(2)))),
                ConstantBinary::Scope {
                    min: Bound::Included(Arc::new(DataValue::Int32(Some(4)))),
                    max: Bound::Excluded(Arc::new(DataValue::Int32(Some(12)))),
                },
            ],
            &sketch,
        );

        assert_eq!(count_1, 9);

        let count_2 = histogram.collect_count(
            &vec![ConstantBinary::Scope {
                min: Bound::Included(Arc::new(DataValue::Int32(Some(4)))),
                max: Bound::Unbounded,
            }],
            &sketch,
        );

        assert_eq!(count_2, 11);

        let count_3 = histogram.collect_count(
            &vec![ConstantBinary::Scope {
                min: Bound::Excluded(Arc::new(DataValue::Int32(Some(7)))),
                max: Bound::Unbounded,
            }],
            &sketch,
        );

        assert_eq!(count_3, 7);

        let count_4 = histogram.collect_count(
            &vec![ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Included(Arc::new(DataValue::Int32(Some(11)))),
            }],
            &sketch,
        );

        assert_eq!(count_4, 12);

        let count_5 = histogram.collect_count(
            &vec![ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(Arc::new(DataValue::Int32(Some(8)))),
            }],
            &sketch,
        );

        assert_eq!(count_5, 8);

        let count_6 = histogram.collect_count(
            &vec![ConstantBinary::Scope {
                min: Bound::Included(Arc::new(DataValue::Int32(Some(2)))),
                max: Bound::Unbounded,
            }],
            &sketch,
        );

        assert_eq!(count_6, 13);

        let count_7 = histogram.collect_count(
            &vec![ConstantBinary::Scope {
                min: Bound::Excluded(Arc::new(DataValue::Int32(Some(1)))),
                max: Bound::Unbounded,
            }],
            &sketch,
        );

        assert_eq!(count_7, 13);

        let count_8 = histogram.collect_count(
            &vec![ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Included(Arc::new(DataValue::Int32(Some(12)))),
            }],
            &sketch,
        );

        assert_eq!(count_8, 13);

        let count_9 = histogram.collect_count(
            &vec![ConstantBinary::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(Arc::new(DataValue::Int32(Some(13)))),
            }],
            &sketch,
        );

        assert_eq!(count_9, 13);

        let count_10 = histogram.collect_count(
            &vec![ConstantBinary::Scope {
                min: Bound::Excluded(Arc::new(DataValue::Int32(Some(0)))),
                max: Bound::Excluded(Arc::new(DataValue::Int32(Some(3)))),
            }],
            &sketch,
        );

        assert_eq!(count_10, 2);

        let count_11 = histogram.collect_count(
            &vec![ConstantBinary::Scope {
                min: Bound::Included(Arc::new(DataValue::Int32(Some(1)))),
                max: Bound::Included(Arc::new(DataValue::Int32(Some(2)))),
            }],
            &sketch,
        );

        assert_eq!(count_11, 2);

        Ok(())
    }
}
