mod create_table;
mod projection;
mod table_scan;
mod insert;
mod values;
mod filter;
mod sort;
mod limit;
pub mod join;

use crate::execution::physical_plan::physical_projection::PhysicalProjection;
use crate::execution::physical_plan::PhysicalPlan;
use crate::execution::volcano_executor::create_table::CreateTable;
use crate::execution::volcano_executor::projection::Projection;
use crate::execution::volcano_executor::table_scan::TableScan;
use crate::execution::ExecutorError;
use crate::storage::StorageImpl;
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use crate::execution::physical_plan::physical_filter::PhysicalFilter;
use crate::execution::physical_plan::physical_hash_join::PhysicalHashJoin;
use crate::execution::physical_plan::physical_insert::PhysicalInsert;
use crate::execution::physical_plan::physical_limit::PhysicalLimit;
use crate::execution::physical_plan::physical_sort::PhysicalSort;
use crate::execution::volcano_executor::filter::Filter;
use crate::execution::volcano_executor::insert::Insert;
use crate::execution::volcano_executor::join::hash_join::HashJoin;
use crate::execution::volcano_executor::limit::Limit;
use crate::execution::volcano_executor::sort::Sort;
use crate::execution::volcano_executor::values::Values;
use crate::planner::operator::join::JoinOperator;

pub type BoxedExecutor = BoxStream<'static, Result<RecordBatch, ExecutorError>>;

pub struct VolcanoExecutor {
    storage: StorageImpl
}

impl VolcanoExecutor {
    pub(crate) fn new(storage: StorageImpl) -> Self {
        Self { storage }
    }

    pub(crate) fn build(&self, plan: PhysicalPlan) -> BoxedExecutor {
        match plan {
            PhysicalPlan::TableScan(op) => {
                match &self.storage {
                    StorageImpl::InMemoryStorage(storage) => TableScan::execute(op, storage.clone())
                }
            }
            PhysicalPlan::Projection(PhysicalProjection { input, exprs, .. }) => {
                let input = self.build(*input);

                Projection::execute(exprs, input)
            }
            PhysicalPlan::CreateTable(op) => match &self.storage {
                StorageImpl::InMemoryStorage(storage) => CreateTable::execute(op, storage.clone()),
            },
            PhysicalPlan::Insert(PhysicalInsert { table_name, input}) => {
                let input = self.build(*input);

                match &self.storage {
                    StorageImpl::InMemoryStorage(storage) =>
                        Insert::execute(table_name, input, storage.clone()),
                }
            }
            PhysicalPlan::Values(op) => Values::execute(op),
            PhysicalPlan::Filter(PhysicalFilter { predicate, input, .. }) => {
                let input = self.build(*input);

                Filter::execute(predicate, input)
            }
            PhysicalPlan::Sort(PhysicalSort {op, input, ..}) => {
                let input = self.build(*input);

                Sort::execute(op.sort_fields, op.limit, input)
            }
            PhysicalPlan::Limit(PhysicalLimit {op,input, ..}) => {
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

    pub async fn try_collect(executor: &mut BoxedExecutor) -> Result<Vec<RecordBatch>, ExecutorError> {
        let mut output = Vec::new();
        while let Some(batch) = executor.try_next().await? {
            output.push(batch);
        }
        Ok(output)
    }
}