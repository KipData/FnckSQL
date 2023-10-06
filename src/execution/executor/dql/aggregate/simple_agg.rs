use crate::execution::executor::dql::aggregate::create_accumulators;
use crate::execution::executor::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::ValueRef;
use futures_async_stream::try_stream;
use itertools::Itertools;
use std::cell::RefCell;

pub struct SimpleAggExecutor {
    pub agg_calls: Vec<ScalarExpression>,
}

impl From<AggregateOperator> for SimpleAggExecutor {
    fn from(AggregateOperator { agg_calls, .. }: AggregateOperator) -> SimpleAggExecutor {
        SimpleAggExecutor { agg_calls }
    }
}

impl<T: Transaction> Executor<T> for SimpleAggExecutor {
    fn execute(self, inputs: Vec<BoxedExecutor>, _transaction: &RefCell<T>) -> BoxedExecutor {
        self._execute(inputs)
    }
}

impl SimpleAggExecutor {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute(self, mut inputs: Vec<BoxedExecutor>) {
        let mut accs = create_accumulators(&self.agg_calls);
        let mut columns_option = None;

        #[for_await]
        for tuple in inputs.remove(0) {
            let tuple = tuple?;

            columns_option.get_or_insert_with(|| {
                self.agg_calls
                    .iter()
                    .map(|expr| expr.output_columns(&tuple))
                    .collect_vec()
            });

            let values: Vec<ValueRef> = self
                .agg_calls
                .iter()
                .map(|expr| match expr {
                    ScalarExpression::AggCall { args, .. } => args[0].eval_column(&tuple),
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
