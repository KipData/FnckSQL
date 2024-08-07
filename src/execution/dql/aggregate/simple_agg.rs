use crate::execution::dql::aggregate::create_accumulators;
use crate::execution::{build_read, Executor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;
use itertools::Itertools;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct SimpleAggExecutor {
    agg_calls: Vec<ScalarExpression>,
    input: LogicalPlan,
}

impl From<(AggregateOperator, LogicalPlan)> for SimpleAggExecutor {
    fn from(
        (AggregateOperator { agg_calls, .. }, input): (AggregateOperator, LogicalPlan),
    ) -> Self {
        SimpleAggExecutor { agg_calls, input }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for SimpleAggExecutor {
    fn execute(self, transaction: &'a T) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let SimpleAggExecutor {
                    agg_calls,
                    mut input,
                } = self;

                let mut accs = throw!(create_accumulators(&agg_calls));
                let schema = input.output_schema().clone();

                let mut coroutine = build_read(input, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple = throw!(tuple);

                    let values: Vec<ValueRef> = throw!(agg_calls
                        .iter()
                        .map(|expr| match expr {
                            ScalarExpression::AggCall { args, .. } => args[0].eval(&tuple, &schema),
                            _ => unreachable!(),
                        })
                        .try_collect());

                    for (acc, value) in accs.iter_mut().zip_eq(values.iter()) {
                        throw!(acc.update_value(value));
                    }
                }
                let values: Vec<ValueRef> =
                    throw!(accs.into_iter().map(|acc| acc.evaluate()).try_collect());

                yield Ok(Tuple { id: None, values });
            },
        )
    }
}
