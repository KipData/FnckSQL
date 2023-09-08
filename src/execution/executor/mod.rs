pub(crate) mod dql;
pub(crate)mod ddl;
pub(crate)mod dml;

use futures::stream::BoxStream;
use futures::TryStreamExt;
use crate::execution::executor::ddl::create_table::CreateTable;
use crate::execution::executor::ddl::drop_table::DropTable;
use crate::execution::executor::dml::delete::Delete;
use crate::execution::executor::dml::insert::Insert;
use crate::execution::executor::dml::update::Update;
use crate::execution::executor::dql::dummy::Dummy;
use crate::execution::executor::dql::filter::Filter;
use crate::execution::executor::dql::join::hash_join::HashJoin;
use crate::execution::executor::dql::limit::Limit;
use crate::execution::executor::dql::projection::Projection;
use crate::execution::executor::dql::seq_scan::SeqScan;
use crate::execution::executor::dql::sort::Sort;
use crate::execution::executor::dql::values::Values;
use crate::execution::ExecutorError;
use crate::planner::LogicalPlan;
use crate::planner::operator::Operator;
use crate::storage::Storage;
use crate::types::tuple::Tuple;

pub type BoxedExecutor = BoxStream<'static, Result<Tuple, ExecutorError>>;

pub trait Executor<S: Storage> {
    fn execute(self, storage: &S) -> BoxedExecutor;
}



//
// impl Executor {
//     pub fn new(storage: KipStorage) -> Executor {
//         Executor {
//             storage
//         }
//     }
//
//     pub fn build(&self, plan: PhysicalPlan) -> BoxedExecutor {
//         match plan {
//             PhysicalPlan::TableScan(op) => {
//                 SeqScan::execute(op, self.storage.clone())
//             }
//             PhysicalPlan::Projection(PhysicalProjection { input, exprs, .. }) => {
//                 let input = self.build(*input);
//
//                 Projection::execute(exprs, input)
//             }
//             PhysicalPlan::Insert(PhysicalInsert { table_name, input}) => {
//                 let input = self.build(*input);
//
//                 Insert::execute(table_name, input, self.storage.clone())
//             }
//             PhysicalPlan::Update(PhysicalUpdate { table_name, input, values}) => {
//                 let input = self.build(*input);
//                 let values = self.build(*values);
//
//                 Update::execute(table_name, input, values, self.storage.clone())
//             }
//             PhysicalPlan::Delete(PhysicalDelete { table_name, input }) => {
//                 let input = self.build(*input);
//
//                 Delete::execute(table_name, input, self.storage.clone())
//             }
//             PhysicalPlan::Values(op) => {
//                 Values::execute(op)
//             }
//             PhysicalPlan::CreateTable(op) => {
//                 CreateTable::execute(op, self.storage.clone())
//             }
//             PhysicalPlan::Filter(PhysicalFilter { predicate, input, .. }) => {
//                 let input = self.build(*input);
//
//                 Filter::execute(predicate, input)
//             }
//             PhysicalPlan::Sort(PhysicalSort {op, input, ..}) => {
//                 let input = self.build(*input);
//
//                 Sort::execute(op.sort_fields, op.limit, input)
//             }
//             PhysicalPlan::Limit(PhysicalLimit {op, input, ..}) => {
//                 let input = self.build(*input);
//
//                 Limit::execute(Some(op.offset), Some(op.limit), input)
//             }
//             PhysicalPlan::HashJoin(PhysicalHashJoin { op, left_input, right_input}) => {
//                 let left_input = self.build(*left_input);
//                 let right_input = self.build(*right_input);
//
//                 let JoinOperator { on, join_type } = op;
//
//                 HashJoin::execute(on, join_type, left_input, right_input)
//             }
//         }
//     }
// }

pub fn build<S: Storage>(plan: LogicalPlan, storage: &S) -> BoxedExecutor {
    let LogicalPlan { operator, mut childrens } = plan;

    match operator {
        Operator::Dummy => Dummy{ }.execute(storage),
        Operator::Aggregate(op) => {
            todo!()
        }
        Operator::Filter(op) => {
            let input = build(childrens.remove(0), storage);

            Filter::from((op, input)).execute(storage)
        }
        Operator::Join(op) => {
            let left_input = build(childrens.remove(0), storage);
            let right_input = build(childrens.remove(0), storage);

            HashJoin::from((op, left_input, right_input)).execute(storage)
        }
        Operator::Project(op) => {
            let input = build(childrens.remove(0), storage);

            Projection::from((op, input)).execute(storage)
        }
        Operator::Scan(op) => {
            SeqScan::from(op).execute(storage)
        }
        Operator::Sort(op) => {
            let input = build(childrens.remove(0), storage);

            Sort::from((op, input)).execute(storage)
        }
        Operator::Limit(op) => {
            let input = build(childrens.remove(0), storage);

            Limit::from((op, input)).execute(storage)
        }
        Operator::Insert(op) => {
            let input = build(childrens.remove(0), storage);

            Insert::from((op, input)).execute(storage)
        }
        Operator::Update(op) => {
            let input = build(childrens.remove(0), storage);
            let values = build(childrens.remove(0), storage);

            Update::from((op, input, values)).execute(storage)
        }
        Operator::Delete(op) => {
            let input = build(childrens.remove(0), storage);

            Delete::from((op, input)).execute(storage)
        }
        Operator::Values(op) => {
            Values::from(op).execute(storage)
        }
        Operator::CreateTable(op) => {
            CreateTable::from(op).execute(storage)
        }
        Operator::DropTable(op) => {
            DropTable::from(op).execute(storage)
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