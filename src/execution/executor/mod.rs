pub(crate) mod ddl;
pub(crate) mod dml;
pub(crate) mod dql;
pub(crate) mod show;

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
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use std::cell::RefCell;

pub type BoxedExecutor = BoxStream<'static, Result<Tuple, ExecutorError>>;

pub trait Executor<T: Transaction> {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor;
}

pub fn build<T: Transaction>(plan: LogicalPlan, transaction: &RefCell<T>) -> BoxedExecutor {
    let LogicalPlan {
        operator,
        mut childrens,
    } = plan;

    match operator {
        Operator::Dummy => Dummy {}.execute(transaction),
        Operator::Aggregate(op) => {
            let input = build(childrens.remove(0), transaction);

            if op.groupby_exprs.is_empty() {
                SimpleAggExecutor::from((op, input)).execute(transaction)
            } else {
                HashAggExecutor::from((op, input)).execute(transaction)
            }
        }
        Operator::Filter(op) => {
            let input = build(childrens.remove(0), transaction);

            Filter::from((op, input)).execute(transaction)
        }
        Operator::Join(op) => {
            let left_input = build(childrens.remove(0), transaction);
            let right_input = build(childrens.remove(0), transaction);

            HashJoin::from((op, left_input, right_input)).execute(transaction)
        }
        Operator::Project(op) => {
            let input = build(childrens.remove(0), transaction);

            Projection::from((op, input)).execute(transaction)
        }
        Operator::Scan(op) => {
            if op.index_by.is_some() {
                IndexScan::from(op).execute(transaction)
            } else {
                SeqScan::from(op).execute(transaction)
            }
        }
        Operator::Sort(op) => {
            let input = build(childrens.remove(0), transaction);

            Sort::from((op, input)).execute(transaction)
        }
        Operator::Limit(op) => {
            let input = build(childrens.remove(0), transaction);

            Limit::from((op, input)).execute(transaction)
        }
        Operator::Insert(op) => {
            let input = build(childrens.remove(0), transaction);

            Insert::from((op, input)).execute(transaction)
        }
        Operator::Update(op) => {
            let input = build(childrens.remove(0), transaction);
            let values = build(childrens.remove(0), transaction);

            Update::from((op, input, values)).execute(transaction)
        }
        Operator::Delete(op) => {
            let input = build(childrens.remove(0), transaction);

            Delete::from((op, input)).execute(transaction)
        }
        Operator::Values(op) => Values::from(op).execute(transaction),
        Operator::CreateTable(op) => CreateTable::from(op).execute(transaction),
        Operator::DropTable(op) => DropTable::from(op).execute(transaction),
        Operator::Truncate(op) => Truncate::from(op).execute(transaction),
        Operator::Show(op) => ShowTables::from(op).execute(transaction),
        Operator::CopyFromFile(op) => CopyFromFile::from(op).execute(transaction),
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
