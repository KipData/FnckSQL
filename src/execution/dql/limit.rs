use crate::execution::{build_read, Executor, ReadExecutor};
use crate::planner::operator::limit::LimitOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction};
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct Limit {
    offset: Option<usize>,
    limit: Option<usize>,
    input: LogicalPlan,
}

impl From<(LimitOperator, LogicalPlan)> for Limit {
    fn from((LimitOperator { offset, limit }, input): (LimitOperator, LogicalPlan)) -> Self {
        Limit {
            offset,
            limit,
            input,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Limit {
    fn execute(
        self,
        cache: (&'a TableCache, &'a StatisticsMetaCache),
        transaction: &'a T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let Limit {
                    offset,
                    limit,
                    input,
                } = self;

                if limit.is_some() && limit.unwrap() == 0 {
                    return;
                }

                let offset_val = offset.unwrap_or(0);
                let offset_limit = offset_val + limit.unwrap_or(1) - 1;

                let mut i = 0;
                let mut coroutine = build_read(input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    i += 1;
                    if i - 1 < offset_val {
                        continue;
                    } else if i - 1 > offset_limit {
                        break;
                    }

                    yield tuple;
                }
            },
        )
    }
}

impl Limit {}
