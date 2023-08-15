use arrow::datatypes::DataType;
use arrow::compute;
use arrow::compute::kernels::cast::cast;
use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array};
use crate::execution_v1::ExecutorError;
use crate::execution_v1::volcano_executor::aggregate::Accumulator;
use crate::types::value::DataValue;

// returns the new value after sum with the new values, taking nullability into account
macro_rules! typed_sum_delta_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let delta = compute::sum(array);
        DataValue::$SCALAR(delta)
    }};
}

// returns the sum of two scalar values, including coercion into $TYPE.
macro_rules! typed_sum {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        DataValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone() as $TYPE),
            (Some(a), Some(b)) => Some(a + (*b as $TYPE)),
        })
    }};
}


pub struct SumAccumulator {
    result: DataValue,
    data_type: DataType,
}

impl SumAccumulator {
    pub fn new(data_type: DataType) -> Self {
        let init_value = DataValue::new_none_value(&data_type).unwrap();
        Self {
            result: init_value,
            data_type,
        }
    }
    fn sum_batch(
        &mut self,
        values: &ArrayRef,
        sum_type: &DataType,
    ) -> Result<DataValue, ExecutorError> {
        let values = cast(values, sum_type)?;
        Ok(match values.data_type() {
            DataType::Int32 => typed_sum_delta_batch!(values, Int32Array, Int32),
            DataType::Int64 => typed_sum_delta_batch!(values, Int64Array, Int64),
            DataType::Float64 => typed_sum_delta_batch!(values, Float64Array, Float64),
            _ => unimplemented!("unsupported sum type: {}", values.data_type()),
        })
    }

}

fn sum_result(l: &DataValue, r: &DataValue) -> DataValue {
    match (l, r) {
        // float64 coerces everything to f64
        (DataValue::Float64(lhs), DataValue::Float64(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (DataValue::Float64(lhs), DataValue::Int64(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (DataValue::Float64(lhs), DataValue::Int32(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        // i64 coerces i* to i64
        (DataValue::Int64(lhs), DataValue::Int64(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        (DataValue::Int64(lhs), DataValue::Int32(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        (DataValue::Int32(lhs), DataValue::Int32(rhs)) => {
            typed_sum!(lhs, rhs, Int32, i32)
        }

        _ => unimplemented!("not expected {:?} and {:?} for sum", l, r),
    }
}



impl Accumulator for SumAccumulator {
    fn update_batch(&mut self, array: &ArrayRef) -> Result<(), ExecutorError> {
        let batch_sum_result = self.sum_batch(array, &self.data_type.clone())?;
        self.result = sum_result(&self.result, &batch_sum_result);
        Ok(())
    }

    fn evaluate(&self) -> Result<DataValue, ExecutorError> {
        Ok(self.result.clone())
    }
}