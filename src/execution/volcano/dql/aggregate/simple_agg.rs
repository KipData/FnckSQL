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
        let mut accs = create_accumulators(&self.agg_calls);
        let mut columns_option = None;

        #[for_await]
        for tuple in build_read(self.input, transaction) {
            let tuple = tuple?;

            columns_option.get_or_insert_with(|| {
                self.agg_calls
                    .iter()
                    .map(|expr| expr.output_column())
                    .collect_vec()
            });

            let values: Vec<ValueRef> = self
                .agg_calls
                .iter()
                .map(|expr| match expr {
                    ScalarExpression::AggCall { args, .. } => args[0].eval(&tuple),
                    _ => unreachable!(),
                })
                .try_collect()?;

            for (acc, value) in accs.iter_mut().zip_eq(values.iter()) {
                acc.update_value(value)?;
            }
        }

        if let Some(columns) = columns_option {
            let values: Vec<ValueRef> = accs.into_iter().map(|acc| acc.evaluate()).try_collect()?;

            yield Tuple {
                id: None,
                columns,
                values,
            };
        }
    }
}
