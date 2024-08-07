pub(crate) mod ddl;
pub(crate) mod dml;
pub(crate) mod dql;
pub(crate) mod marco;

use self::ddl::add_column::AddColumn;
use self::dql::join::nested_loop_join::NestedLoopJoin;
use crate::errors::DatabaseError;
use crate::execution::ddl::create_index::CreateIndex;
use crate::execution::ddl::create_table::CreateTable;
use crate::execution::ddl::drop_column::DropColumn;
use crate::execution::ddl::drop_table::DropTable;
use crate::execution::ddl::truncate::Truncate;
use crate::execution::dml::analyze::Analyze;
use crate::execution::dml::copy_from_file::CopyFromFile;
use crate::execution::dml::delete::Delete;
use crate::execution::dml::insert::Insert;
use crate::execution::dml::update::Update;
use crate::execution::dql::aggregate::hash_agg::HashAggExecutor;
use crate::execution::dql::aggregate::simple_agg::SimpleAggExecutor;
use crate::execution::dql::describe::Describe;
use crate::execution::dql::dummy::Dummy;
use crate::execution::dql::explain::Explain;
use crate::execution::dql::filter::Filter;
use crate::execution::dql::index_scan::IndexScan;
use crate::execution::dql::join::hash_join::HashJoin;
use crate::execution::dql::limit::Limit;
use crate::execution::dql::projection::Projection;
use crate::execution::dql::seq_scan::SeqScan;
use crate::execution::dql::show_table::ShowTables;
use crate::execution::dql::sort::Sort;
use crate::execution::dql::union::Union;
use crate::execution::dql::values::Values;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::IndexInfo;
use crate::types::tuple::Tuple;
use std::ops::{Coroutine, CoroutineState};
use std::pin::Pin;

pub type Executor<'a> =
    Box<dyn Coroutine<Yield = Result<Tuple, DatabaseError>, Return = ()> + 'a + Unpin>;

pub trait ReadExecutor<'a, T: Transaction + 'a> {
    fn execute(self, transaction: &'a T) -> Executor<'a>;
}

pub trait WriteExecutor<'a, T: Transaction + 'a> {
    fn execute_mut(self, transaction: &'a mut T) -> Executor<'a>;
}

pub fn build_read<'a, T: Transaction + 'a>(plan: LogicalPlan, transaction: &'a T) -> Executor<'a> {
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
                JoinCondition::On { on, .. }
                    if !on.is_empty() && plan.physical_option == Some(PhysicalOption::HashJoin) =>
                {
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

pub fn build_write<'a, T: Transaction + 'a>(
    plan: LogicalPlan,
    transaction: &'a mut T,
) -> Executor<'a> {
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

pub fn try_collect(mut executor: Executor) -> Result<Vec<Tuple>, DatabaseError> {
    let mut output = Vec::new();

    while let CoroutineState::Yielded(tuple) = Pin::new(&mut executor).resume(()) {
        output.push(tuple?);
    }
    Ok(output)
}
