use crate::execution::{build_read, Executor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct Filter {
    predicate: ScalarExpression,
    input: LogicalPlan,
}

impl From<(FilterOperator, LogicalPlan)> for Filter {
    fn from((FilterOperator { predicate, .. }, input): (FilterOperator, LogicalPlan)) -> Self {
        Filter { predicate, input }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Filter {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let Filter {
                    predicate,
                    mut input,
                } = self;

                let schema = input.output_schema().clone();

                let mut coroutine = build_read(input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple = throw!(tuple);

                    if throw!(throw!(predicate.eval(Some((&tuple, &schema)))).is_true()) {
                        yield Ok(tuple);
                    }
                }
            },
        )
    }
}
