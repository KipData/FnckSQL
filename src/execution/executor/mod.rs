pub(crate) mod dql;
pub(crate)mod ddl;
pub(crate)mod dml;
pub(crate) mod show;

use futures::stream::BoxStream;
use futures::TryStreamExt;
use crate::execution::executor::ddl::create_table::CreateTable;
use crate::execution::executor::ddl::drop_table::DropTable;
use crate::execution::executor::ddl::truncate::Truncate;
use crate::execution::executor::dml::copy_from_file::CopyFromFile;
use crate::execution::executor::dml::delete::Delete;
use crate::execution::executor::dml::insert::Insert;
use crate::execution::executor::dml::update::Update;
use crate::execution::executor::dql::aggregate::hash_agg::HashAggExecutor;
use crate::execution::executor::dql::aggregate::simple_agg::SimpleAggExecutor;
use crate::execution::executor::dql::dummy::Dummy;
use crate::execution::executor::dql::filter::Filter;
use crate::execution::executor::dql::index_scan::IndexScan;
use crate::execution::executor::dql::join::hash_join::HashJoin;
use crate::execution::executor::dql::limit::Limit;
use crate::execution::executor::dql::projection::Projection;
use crate::execution::executor::dql::seq_scan::SeqScan;
use crate::execution::executor::dql::sort::Sort;
use crate::execution::executor::dql::values::Values;
use crate::execution::executor::show::show_table::ShowTables;
use crate::execution::ExecutorError;
use crate::planner::LogicalPlan;
use crate::planner::operator::Operator;
use crate::storage::Storage;
use crate::types::tuple::Tuple;

pub type BoxedExecutor = BoxStream<'static, Result<Tuple, ExecutorError>>;

pub trait Executor<S: Storage> {
    fn execute(self, storage: &S) -> BoxedExecutor;
}

pub fn build<S: Storage>(plan: LogicalPlan, storage: &S) -> BoxedExecutor {
    let LogicalPlan { operator, mut childrens } = plan;

    match operator {
        Operator::Dummy => Dummy{ }.execute(storage),
        Operator::Aggregate(op) => {
            let input = build(childrens.remove(0), storage);

            if op.groupby_exprs.is_empty() {
                SimpleAggExecutor::from((op, input)).execute(storage)
            } else {
                HashAggExecutor::from((op, input)).execute(storage)
            }
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
            if op.index_by.is_some() {
                IndexScan::from(op).execute(storage)
            } else {
                SeqScan::from(op).execute(storage)
            }
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
        Operator::Truncate(op) => {
            Truncate::from(op).execute(storage)
        }
        Operator::Show(op) => {
            ShowTables::from(op).execute(storage)
        }
        Operator::CopyFromFile(op) => {
            CopyFromFile::from(op).execute(storage)
        }
        #[warn(unused_assignments)]
        Operator::CopyToFile(_op) => {
            todo!()
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