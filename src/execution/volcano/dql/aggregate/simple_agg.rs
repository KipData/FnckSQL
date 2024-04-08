use crate::errors::DatabaseError;
use crate::execution::volcano::dql::aggregate::create_accumulators;
use crate::execution::volcano::{build_read, BoxedExecutor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;
use futures_async_stream::try_stream;
use itertools::Itertools;

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

impl<T: Transaction> ReadExecutor<T> for SimpleAggExecutor {
    fn execute(self, transaction: &T) -> BoxedExecutor {
        self._execute(transaction)
    }
}

impl SimpleAggExecutor {
    #[try_stream(boxed, ok = Tuple, error = DatabaseError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
        let SimpleAggExecutor {
            agg_calls,
            mut input,
        } = self;
        let mut accs = create_accumulators(&agg_calls)?;
        let schema = input.output_schema().clone();

        #[for_await]
        for tuple in build_read(input, transaction) {
            let tuple = tuple?;

            let values: Vec<ValueRef> = agg_calls
                .iter()
                .map(|expr| match expr {
                    ScalarExpression::AggCall { args, .. } => args[0].eval(&tuple, &schema),
                    _ => unreachable!(),
                })
                .try_collect()?;

            for (acc, value) in accs.iter_mut().zip_eq(values.iter()) {
                acc.update_value(value)?;
            }
        }
        let values: Vec<ValueRef> = accs.into_iter().map(|acc| acc.evaluate()).try_collect()?;

        yield Tuple { id: None, values };
    }
}
