use crate::execution::{build_read, Executor, ReadExecutor};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct Union {
    left_input: LogicalPlan,
    right_input: LogicalPlan,
}

impl From<(LogicalPlan, LogicalPlan)> for Union {
    fn from((left_input, right_input): (LogicalPlan, LogicalPlan)) -> Self {
        Union {
            left_input,
            right_input,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Union {
    fn execute(self, transaction: &'a T) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let Union {
                    left_input,
                    right_input,
                } = self;
                let mut coroutine = build_read(left_input, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    yield tuple;
                }
                let mut coroutine = build_read(right_input, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    yield tuple;
                }
            },
        )
    }
}
