use arrow::array::ArrayRef;
use crate::execution_v1::ExecutorError;
use crate::execution_v1::volcano_executor::aggregate::sum::SumAccumulator;
use crate::expression::agg::AggKind;
use crate::expression::ScalarExpression;
use crate::types::value::DataValue;

pub mod simple_agg;
mod  sum;

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