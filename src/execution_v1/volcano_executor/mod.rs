mod create_table;
mod projection;
mod table_scan;

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
            _ => {
                unimplemented!()
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