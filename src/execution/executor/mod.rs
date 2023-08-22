pub(crate) mod dql;
pub(crate)mod ddl;
pub(crate)mod dml;

use futures::stream::BoxStream;
use futures::TryStreamExt;
use crate::execution::physical_plan::physical_filter::PhysicalFilter;
use crate::execution::physical_plan::physical_hash_join::PhysicalHashJoin;
use crate::execution::physical_plan::physical_insert::PhysicalInsert;
use crate::execution::physical_plan::physical_limit::PhysicalLimit;
use crate::execution::physical_plan::physical_projection::PhysicalProjection;
use crate::execution::physical_plan::physical_sort::PhysicalSort;
use crate::execution::physical_plan::PhysicalPlan;
use crate::execution::executor::ddl::create::CreateTable;
use crate::execution::executor::dql::filter::Filter;
use crate::execution::executor::dml::insert::Insert;
use crate::execution::executor::dql::join::hash_join::HashJoin;
use crate::execution::executor::dql::limit::Limit;
use crate::execution::executor::dql::projection::Projection;
use crate::execution::executor::dql::seq_scan::SeqScan;
use crate::execution::executor::dql::sort::Sort;
use crate::execution::executor::dql::values::Values;
use crate::execution::ExecutorError;
use crate::planner::operator::join::JoinOperator;
use crate::storage::memory::MemStorage;
use crate::types::tuple::Tuple;

pub type BoxedExecutor = BoxStream<'static, Result<Tuple, ExecutorError>>;

pub struct Executor {
    storage: MemStorage
}

impl Executor {
    pub fn new(storage: MemStorage) -> Executor {
        Executor {
            storage
        }
    }

    pub fn build(&self, plan: PhysicalPlan) -> BoxedExecutor {
        match plan {
            PhysicalPlan::TableScan(op) => {
                SeqScan::execute(op, self.storage.clone())
            }
            PhysicalPlan::Projection(PhysicalProjection { input, exprs, .. }) => {
                let input = self.build(*input);

                Projection::execute(exprs, input)
            }
            PhysicalPlan::Insert(PhysicalInsert { table_id, input}) => {
                let input = self.build(*input);

                Insert::execute(table_id, input, self.storage.clone())
            }
            PhysicalPlan::Values(op) => {
                Values::execute(op)
            }
            PhysicalPlan::CreateTable(op) => {
                CreateTable::execute(op, self.storage.clone())
            }
            PhysicalPlan::Filter(PhysicalFilter { predicate, input, .. }) => {
                let input = self.build(*input);

                Filter::execute(predicate, input)
            }
            PhysicalPlan::Sort(PhysicalSort {op, input, ..}) => {
                let input = self.build(*input);

                Sort::execute(op.sort_fields, op.limit, input)
            }
            PhysicalPlan::Limit(PhysicalLimit {op, input, ..}) => {
                let input = self.build(*input);

                Limit::execute(Some(op.offset), Some(op.limit), input)
            }
            PhysicalPlan::HashJoin(PhysicalHashJoin { op, left_input, right_input}) => {
                let left_input = self.build(*left_input);
                let right_input = self.build(*right_input);

                let JoinOperator { on, join_type } = op;

                HashJoin::execute(on, join_type, left_input, right_input)
            }
        }
    }
}

pub async fn try_collect(executor: &mut BoxedExecutor) -> Result<Vec<Tuple>, ExecutorError> {
    let mut output = Vec::new();

    while let Some(tuple) = executor.try_next().await? {
        output.push(tuple);
    }
    Ok(output)
}