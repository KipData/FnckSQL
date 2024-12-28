use crate::execution::{Executor, ReadExecutor};
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use sqlparser::ast::CharLengthUnits;

pub struct Explain {
    plan: LogicalPlan,
}

impl From<LogicalPlan> for Explain {
    fn from(plan: LogicalPlan) -> Self {
        Explain { plan }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Explain {
    fn execute(
        self,
        _: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        _: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let values = vec![DataValue::Utf8 {
                    value: self.plan.explain(0),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }];

                yield Ok(Tuple::new(None, values));
            },
        )
    }
}
