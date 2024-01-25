pub(crate) mod ddl;
pub(crate) mod dml;
pub(crate) mod dql;
pub(crate) mod show;

use crate::execution::volcano::ddl::create_table::CreateTable;
use crate::execution::volcano::ddl::drop_column::DropColumn;
use crate::execution::volcano::ddl::drop_table::DropTable;
use crate::execution::volcano::ddl::truncate::Truncate;
use crate::execution::volcano::dml::analyze::Analyze;
use crate::execution::volcano::dml::copy_from_file::CopyFromFile;
use crate::execution::volcano::dml::delete::Delete;
use crate::execution::volcano::dml::insert::Insert;
use crate::execution::volcano::dml::update::Update;
use crate::execution::volcano::dql::aggregate::hash_agg::HashAggExecutor;
use crate::execution::volcano::dql::aggregate::simple_agg::SimpleAggExecutor;
use crate::execution::volcano::dql::dummy::Dummy;
use crate::execution::volcano::dql::filter::Filter;
use crate::execution::volcano::dql::join::hash_join::HashJoin;
use crate::execution::volcano::dql::limit::Limit;
use crate::execution::volcano::dql::projection::Projection;
use crate::execution::volcano::dql::seq_scan::SeqScan;
use crate::execution::volcano::dql::sort::Sort;
use crate::execution::volcano::dql::values::Values;
use crate::execution::volcano::show::show_table::ShowTables;
use crate::execution::ExecutorError;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use std::cell::RefCell;

use self::ddl::add_column::AddColumn;

pub type BoxedExecutor = BoxStream<'static, Result<Tuple, ExecutorError>>;

pub trait Executor<T: Transaction> {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor;
}

pub fn build_stream<T: Transaction>(plan: LogicalPlan, transaction: &RefCell<T>) -> BoxedExecutor {
    let LogicalPlan {
        operator,
        mut childrens,
    } = plan;

    match operator {
        Operator::Dummy => Dummy {}.execute(transaction),
        Operator::Aggregate(op) => {
            let input = build_stream(childrens.remove(0), transaction);

            if op.groupby_exprs.is_empty() {
                SimpleAggExecutor::from((op, input)).execute(transaction)
            } else {
                HashAggExecutor::from((op, input)).execute(transaction)
            }
        }
        Operator::Filter(op) => {
            let input = build_stream(childrens.remove(0), transaction);

            Filter::from((op, input)).execute(transaction)
        }
        Operator::Join(op) => {
            let left_input = build_stream(childrens.remove(0), transaction);
            let right_input = build_stream(childrens.remove(0), transaction);

            HashJoin::from((op, left_input, right_input)).execute(transaction)
        }
        Operator::Project(op) => {
            let input = build_stream(childrens.remove(0), transaction);

            Projection::from((op, input)).execute(transaction)
        }
        Operator::Scan(op) => {
            SeqScan::from(op).execute(transaction)
            // Fixme
            // if op.index_infos.is_empty() {
            //     SeqScan::from(op).execute(transaction)
            // } else {
            //     IndexScan::from(op).execute(transaction)
            // }
        }
        Operator::Sort(op) => {
            let input = build_stream(childrens.remove(0), transaction);

            Sort::from((op, input)).execute(transaction)
        }
        Operator::Limit(op) => {
            let input = build_stream(childrens.remove(0), transaction);

            Limit::from((op, input)).execute(transaction)
        }
        Operator::Insert(op) => {
            let input = build_stream(childrens.remove(0), transaction);

            Insert::from((op, input)).execute(transaction)
        }
        Operator::Update(op) => {
            let input = build_stream(childrens.remove(0), transaction);
            let values = build_stream(childrens.remove(0), transaction);

            Update::from((op, input, values)).execute(transaction)
        }
        Operator::Delete(op) => {
            let input = build_stream(childrens.remove(0), transaction);

            Delete::from((op, input)).execute(transaction)
        }
        Operator::Values(op) => Values::from(op).execute(transaction),
        Operator::AddColumn(op) => {
            let input = build_stream(childrens.remove(0), transaction);
            AddColumn::from((op, input)).execute(transaction)
        }
        Operator::DropColumn(op) => {
            let input = build_stream(childrens.remove(0), transaction);
            DropColumn::from((op, input)).execute(transaction)
        }
        Operator::CreateTable(op) => CreateTable::from(op).execute(transaction),
        Operator::DropTable(op) => DropTable::from(op).execute(transaction),
        Operator::Truncate(op) => Truncate::from(op).execute(transaction),
        Operator::Show(op) => ShowTables::from(op).execute(transaction),
        Operator::CopyFromFile(op) => CopyFromFile::from(op).execute(transaction),
        #[warn(unused_assignments)]
        Operator::CopyToFile(_op) => {
            todo!()
        }
        Operator::Analyze(op) => {
            let input = build_stream(childrens.remove(0), transaction);

            Analyze::from((op, input)).execute(transaction)
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
