mod avg;
mod count;
pub mod hash_agg;
mod min_max;
pub mod simple_agg;
mod sum;

use crate::execution::volcano::dql::aggregate::avg::AvgAccumulator;
use crate::execution::volcano::dql::aggregate::count::{
    CountAccumulator, DistinctCountAccumulator,
};
use crate::execution::volcano::dql::aggregate::min_max::MinMaxAccumulator;
use crate::execution::volcano::dql::aggregate::sum::{DistinctSumAccumulator, SumAccumulator};
use crate::execution::ExecutorError;
use crate::expression::agg::AggKind;
use crate::expression::ScalarExpression;
use crate::types::value::ValueRef;

/// Tips: Idea for sqlrs
/// An accumulator represents a stateful object that lives throughout the evaluation of multiple
/// rows and generically accumulates values.
pub trait Accumulator: Send + Sync {
    /// updates the accumulator's state from a vector of arrays.
    fn update_value(&mut self, value: &ValueRef) -> Result<(), ExecutorError>;

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<ValueRef, ExecutorError>;
}

fn create_accumulator(expr: &ScalarExpression) -> Box<dyn Accumulator> {
    if let ScalarExpression::AggCall {
        kind, ty, distinct, ..
    } = expr
    {
        match (kind, distinct) {
            (AggKind::Count, false) => Box::new(CountAccumulator::new()),
            (AggKind::Count, true) => Box::new(DistinctCountAccumulator::new()),
            (AggKind::Sum, false) => Box::new(SumAccumulator::new(ty)),
            (AggKind::Sum, true) => Box::new(DistinctSumAccumulator::new(ty)),
            (AggKind::Min, _) => Box::new(MinMaxAccumulator::new(ty, false)),
            (AggKind::Max, _) => Box::new(MinMaxAccumulator::new(ty, true)),
            (AggKind::Avg, _) => Box::new(AvgAccumulator::new(ty)),
        }
    } else {
        unreachable!(
            "create_accumulator called with non-aggregate expression {:?}",
            expr
        );
    }
}

pub(crate) fn create_accumulators(exprs: &[ScalarExpression]) -> Vec<Box<dyn Accumulator>> {
    exprs.iter().map(create_accumulator).collect()
}
