mod dml;
mod ddl;

use futures::stream::BoxStream;
use futures::TryStreamExt;
use crate::execution_ap::physical_plan::physical_filter::PhysicalFilter;
use crate::execution_ap::physical_plan::physical_insert::PhysicalInsert;
use crate::execution_ap::physical_plan::physical_limit::PhysicalLimit;
use crate::execution_ap::physical_plan::physical_projection::PhysicalProjection;
use crate::execution_ap::physical_plan::physical_sort::PhysicalSort;
use crate::execution_ap::physical_plan::PhysicalPlan;
use crate::execution_tp::executor::ddl::create::CreateTable;
use crate::execution_tp::executor::dml::filter::Filter;
use crate::execution_tp::executor::dml::insert::Insert;
use crate::execution_tp::executor::dml::limit::Limit;
use crate::execution_tp::executor::dml::projection::Projection;
use crate::execution_tp::executor::dml::seq_scan::SeqScan;
use crate::execution_tp::executor::dml::sort::Sort;
use crate::execution_tp::executor::dml::values::Values;
use crate::execution_tp::ExecutorError;
use crate::storage_tp::memory::MemStorage;
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
            _ => todo!()
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