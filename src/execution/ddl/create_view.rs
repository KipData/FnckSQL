use crate::execution::{Executor, WriteExecutor};
use crate::planner::operator::create_view::CreateViewOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple_builder::TupleBuilder;
use std::sync::Arc;

pub struct CreateView {
    op: CreateViewOperator,
    view_cache: Arc<ViewCache>,
}

impl From<(CreateViewOperator, Arc<ViewCache>)> for CreateView {
    fn from((op, view_cache): (CreateViewOperator, Arc<ViewCache>)) -> Self {
        CreateView { op, view_cache }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CreateView {
    fn execute_mut(
        self,
        _: (&'a TableCache, &'a StatisticsMetaCache),
        transaction: &'a mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let CreateViewOperator { view, or_replace } = self.op;

                let result_tuple = TupleBuilder::build_result(format!("{}", view.name));
                throw!(transaction.create_view(&self.view_cache, view, or_replace));

                yield Ok(result_tuple);
            },
        )
    }
}
