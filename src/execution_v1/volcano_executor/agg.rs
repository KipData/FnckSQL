use arrow::record_batch::RecordBatch;
use futures_async_stream::try_stream;
use crate::execution_v1::ExecutorError;
use crate::execution_v1::volcano_executor::BoxedExecutor;
use crate::expression::ScalarExpression;
use crate::expression::agg::AggKind;
use arrow::compute;
use arrow::compute::kernels::cast::cast;
use crate::types::value::DataValue;
use arrow::datatypes::DataType;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array};
use itertools::Itertools;

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


pub struct Agg{}

impl Agg {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(exprs: Vec<ScalarExpression>, input: BoxedExecutor){
        let mut acc = create_accumulators(&exprs);

        let mut agg_fileds: Option<Vec<Field>> = None;
        #[for_await]
        for batch in input{
            let batch = batch?;
             // only support one epxrssion in aggregation, not supported example: `sum(distinct a)`
            let columns: Result<Vec<_>, ExecutorError> = exprs
                .iter()
                .map(|expr| match expr {
                    ScalarExpression::AggCall { args, .. } => {
                        // TODO: Only single-argument aggregate functions are supported
                        // 处理 ScalarExpression::AggCall 类型的表达式
                        args[0].eval_column(&batch)
                    }
                    // 处理其他类型的表达式
                    _ => unimplemented!(),
                })
                .try_collect();
            // build new schema for aggregation result
            if agg_fileds.is_none() {
                agg_fileds = Some(
                    exprs.iter()
                        .map(|expr| expr.output_columns().to_field())
                        .collect(),
                )
            }
            let columns = columns?;
            // sumAcc[0].update_batch(&columns[0]);
            for (acc, column) in acc.iter_mut().zip_eq(columns.iter()) {
                acc.update_batch(column)?;
            }
        }
        let mut columns: Vec<ArrayRef> = Vec::new();
        for acc in acc.iter() {
            let res = acc.evaluate()?;
            columns.push(res.to_array_of_size(1));
        }
        let schema = SchemaRef::new(Schema::new(agg_fileds.unwrap()));
        yield RecordBatch::try_new(schema, columns)?;

    }
}

/// Evaluate the bound expr on the given record batch.
/// The core computation logic directly used arrow compute kernels in arrow::compute::kernels.




/// An accumulator represents a stateful object that lives throughout the evaluation of multiple
/// rows and generically accumulates values.
pub trait Accumulator: Send + Sync {
    /// updates the accumulator's state from a vector of arrays.
    fn update_batch(&mut self, array: &ArrayRef) -> Result<(), ExecutorError>;

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<DataValue, ExecutorError>;
}

fn create_accumulators(exprs: &[ScalarExpression]) -> Vec<Box<dyn Accumulator>> {
    exprs.iter().map(create_accumulator).collect()
}

fn create_accumulator(expr: &ScalarExpression) -> Box<dyn Accumulator> {
    if let ScalarExpression::AggCall{kind,ty,..} = expr {
        match kind {
            AggKind::Count => todo!(),
            AggKind::Sum => {
                Box::new(SumAccumulator::new((*ty).clone().into()))
            }
            AggKind::Min => todo!(),
            AggKind::Max => todo!(),
            AggKind::Avg => todo!(),
            AggKind::RowCount => todo!(),
        }
    } else {
        unreachable!(
            "create_accumulator called with non-aggregate expression {:?}",
            expr
        );
    }
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