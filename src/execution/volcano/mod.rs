pub(crate) mod ddl;
pub(crate) mod dml;
pub(crate) mod dql;

use crate::errors::DatabaseError;
use crate::execution::volcano::ddl::create_index::CreateIndex;
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
use crate::execution::volcano::dql::describe::Describe;
use crate::execution::volcano::dql::dummy::Dummy;
use crate::execution::volcano::dql::explain::Explain;
use crate::execution::volcano::dql::filter::Filter;
use crate::execution::volcano::dql::index_scan::IndexScan;
use crate::execution::volcano::dql::join::hash_join::HashJoin;
use crate::execution::volcano::dql::limit::Limit;
use crate::execution::volcano::dql::projection::Projection;
use crate::execution::volcano::dql::seq_scan::SeqScan;
use crate::execution::volcano::dql::show_table::ShowTables;
use crate::execution::volcano::dql::sort::Sort;
use crate::execution::volcano::dql::union::Union;
use crate::execution::volcano::dql::values::Values;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::IndexInfo;
use crate::types::tuple::Tuple;
use futures::stream::BoxStream;
use futures::TryStreamExt;

use self::ddl::add_column::AddColumn;
use self::dql::join::nested_loop_join::NestedLoopJoin;

pub type BoxedExecutor<'a> = BoxStream<'a, Result<Tuple, DatabaseError>>;

pub trait ReadExecutor<T: Transaction> {
    fn execute(self, transaction: &T) -> BoxedExecutor;
}

pub trait WriteExecutor<T: Transaction> {
    fn execute_mut(self, transaction: &mut T) -> BoxedExecutor;
}

pub fn build_read<T: Transaction>(plan: LogicalPlan, transaction: &T) -> BoxedExecutor {
    let LogicalPlan {
        operator,
        mut childrens,
        ..
    } = plan;

    match operator {
        Operator::Dummy => Dummy {}.execute(transaction),
        Operator::Aggregate(op) => {
            let input = childrens.pop().unwrap();

            if op.groupby_exprs.is_empty() {
                SimpleAggExecutor::from((op, input)).execute(transaction)
            } else {
                HashAggExecutor::from((op, input)).execute(transaction)
            }
        }
        Operator::Filter(op) => {
            let input = childrens.pop().unwrap();

            Filter::from((op, input)).execute(transaction)
        }
        Operator::Join(op) => {
            let right_input = childrens.pop().unwrap();
            let left_input = childrens.pop().unwrap();

            match &op.on {
                JoinCondition::On { on, .. } if !on.is_empty() => {
                    HashJoin::from((op, left_input, right_input)).execute(transaction)
                }
                _ => NestedLoopJoin::from((op, left_input, right_input)).execute(transaction),
            }
        }
        Operator::Project(op) => {
            let input = childrens.pop().unwrap();

            Projection::from((op, input)).execute(transaction)
        }
        Operator::Scan(op) => {
            if let Some(PhysicalOption::IndexScan(IndexInfo {
                meta,
                range: Some(range),
            })) = plan.physical_option
            {
                IndexScan::from((op, meta, range)).execute(transaction)
            } else {
                SeqScan::from(op).execute(transaction)
            }
        }
        Operator::Sort(op) => {
            let input = childrens.pop().unwrap();

            Sort::from((op, input)).execute(transaction)
        }
        Operator::Limit(op) => {
            let input = childrens.pop().unwrap();

            Limit::from((op, input)).execute(transaction)
        }
        Operator::Values(op) => Values::from(op).execute(transaction),
        Operator::Show => ShowTables.execute(transaction),
        Operator::Explain => {
            let input = childrens.pop().unwrap();

            Explain::from(input).execute(transaction)
        }
        Operator::Describe(op) => Describe::from(op).execute(transaction),
        Operator::Union(_) => {
            let right_input = childrens.pop().unwrap();
            let left_input = childrens.pop().unwrap();

            Union::from((left_input, right_input)).execute(transaction)
        }
        _ => unreachable!(),
    }
}

pub fn build_write<T: Transaction>(plan: LogicalPlan, transaction: &mut T) -> BoxedExecutor {
    let LogicalPlan {
        operator,
        mut childrens,
        physical_option,
        _output_schema_ref,
    } = plan;

    match operator {
        Operator::Insert(op) => {
            let input = childrens.pop().unwrap();

            Insert::from((op, input)).execute_mut(transaction)
        }
        Operator::Update(op) => {
            let values = childrens.pop().unwrap();
            let input = childrens.pop().unwrap();

            Update::from((op, input, values)).execute_mut(transaction)
        }
        Operator::Delete(op) => {
            let input = childrens.pop().unwrap();

            Delete::from((op, input)).execute_mut(transaction)
        }
        Operator::AddColumn(op) => {
            let input = childrens.pop().unwrap();
            AddColumn::from((op, input)).execute_mut(transaction)
        }
        Operator::DropColumn(op) => {
            let input = childrens.pop().unwrap();
            DropColumn::from((op, input)).execute_mut(transaction)
        }
        Operator::CreateTable(op) => CreateTable::from(op).execute_mut(transaction),
        Operator::CreateIndex(op) => {
            let input = childrens.pop().unwrap();

            CreateIndex::from((op, input)).execute_mut(transaction)
        }
        Operator::DropTable(op) => DropTable::from(op).execute_mut(transaction),
        Operator::Truncate(op) => Truncate::from(op).execute_mut(transaction),
        Operator::CopyFromFile(op) => CopyFromFile::from(op).execute_mut(transaction),
        #[warn(unused_assignments)]
        Operator::CopyToFile(_op) => {
            todo!()
        }
        Operator::Analyze(op) => {
            let input = childrens.pop().unwrap();

            Analyze::from((op, input)).execute_mut(transaction)
        }
        operator => build_read(
            LogicalPlan {
                operator,
                childrens,
                physical_option,
                _output_schema_ref,
            },
            transaction,
        ),
    }
}

pub async fn try_collect<'a>(
    executor: &mut BoxedExecutor<'a>,
) -> Result<Vec<Tuple>, DatabaseError> {
    let mut output = Vec::new();

    while let Some(tuple) = executor.try_next().await? {
        output.push(tuple);
    }
    Ok(output)
}
