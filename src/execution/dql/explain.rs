use crate::execution::{Executor, ReadExecutor};
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction};
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use sqlparser::ast::CharLengthUnits;
use std::sync::Arc;

pub struct Explain {
    plan: LogicalPlan,
}

impl From<LogicalPlan> for Explain {
    fn from(plan: LogicalPlan) -> Self {
        Explain { plan }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Explain {
    fn execute(self, _: (&'a TableCache, &'a StatisticsMetaCache), _: &T) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let values = vec![Arc::new(DataValue::Utf8 {
                    value: Some(self.plan.explain(0)),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                })];

                yield Ok(Tuple { id: None, values });
            },
        )
    }
}
