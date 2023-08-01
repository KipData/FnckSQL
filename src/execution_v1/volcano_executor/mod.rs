mod create_table;
mod projection;
mod table_scan;
mod insert;
mod values;
mod filter;
mod sort;

use crate::execution_v1::physical_plan::physical_projection::PhysicalProjection;
use crate::execution_v1::physical_plan::PhysicalOperator;
use crate::execution_v1::volcano_executor::create_table::CreateTable;
use crate::execution_v1::volcano_executor::projection::Projection;
use crate::execution_v1::volcano_executor::table_scan::TableScan;
use crate::execution_v1::ExecutorError;
use crate::storage::StorageImpl;
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use crate::execution_v1::physical_plan::physical_filter::PhysicalFilter;
use crate::execution_v1::physical_plan::physical_insert::PhysicalInsert;
use crate::execution_v1::physical_plan::physical_sort::PhysicalSort;
use crate::execution_v1::volcano_executor::filter::Filter;
use crate::execution_v1::volcano_executor::insert::Insert;
use crate::execution_v1::volcano_executor::sort::Sort;
use crate::execution_v1::volcano_executor::values::Values;

pub type BoxedExecutor = BoxStream<'static, Result<RecordBatch, ExecutorError>>;

pub struct VolcanoExecutor {
    storage: StorageImpl
}

impl VolcanoExecutor {
    pub(crate) fn new(storage: StorageImpl) -> Self {
        Self { storage }
    }

    pub(crate) fn build(&self, plan: PhysicalOperator) -> BoxedExecutor {
        match plan {
            PhysicalOperator::TableScan(op) => {
                match &self.storage {
                    StorageImpl::InMemoryStorage(storage) => TableScan::execute(op, storage.clone())
                }
            }
            PhysicalOperator::Projection(PhysicalProjection { input, exprs, .. }) => {
                let input = self.build(*input);

                Projection::execute(exprs, input)
            }
            PhysicalOperator::CreateTable(op) => match &self.storage {
                StorageImpl::InMemoryStorage(storage) => CreateTable::execute(op, storage.clone()),
            },
            PhysicalOperator::Insert(PhysicalInsert { table_name, input}) => {
                let input = self.build(*input);

                match &self.storage {
                    StorageImpl::InMemoryStorage(storage) =>
                        Insert::execute(table_name, input, storage.clone()),
                }
            }
            PhysicalOperator::Values(op) => Values::execute(op),
            PhysicalOperator::Filter(PhysicalFilter { predicate, input, .. }) => {
                let input = self.build(*input);

                Filter::execute(predicate, input)
            }
            PhysicalOperator::Sort(PhysicalSort {op, input, ..}) => {
                let input = self.build(*input);

                Sort::execute(op.sort_fields, op.limit, input)
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