mod count;
pub mod simple_agg;

use crate::execution::executor::dql::aggregate::count::{CountAccumulator, DistinctCountAccumulator};
use crate::execution::ExecutorError;
use crate::expression::agg::AggKind;
use crate::expression::ScalarExpression;
use crate::types::value::{DataValue, ValueRef};

/// Tips: Idea for sqlrs
/// An accumulator represents a stateful object that lives throughout the evaluation of multiple
/// rows and generically accumulates values.
pub trait Accumulator: Send + Sync {
    /// updates the accumulator's state from a vector of arrays.
    fn update_batch(&mut self, value: &ValueRef) -> Result<(), ExecutorError>;

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<DataValue, ExecutorError>;
}

fn create_accumulator(expr: &ScalarExpression) -> Box<dyn Accumulator> {
    if let ScalarExpression::AggCall { kind, args, ty, distinct } = expr {
        match (kind, distinct) {
            (AggKind::Count, false) => Box::new(CountAccumulator::new()),
            (AggKind::Count, true) => Box::new(DistinctCountAccumulator::new()),
            (AggKind::Sum, false) => todo!(),
            (AggKind::Sum, true) => todo!(),
            (AggKind::Min, _) => todo!(),
            (AggKind::Max, _) => todo!(),
            (AggKind::Avg, _) => todo!(),
        }
    } else {
        unreachable!(
            "create_accumulator called with non-aggregate expression {:?}",
            expr
        );
    }
}

fn create_accumulators(exprs: &[ScalarExpression]) -> Vec<Box<dyn Accumulator>> {
    exprs.iter().map(create_accumulator).collect()
}