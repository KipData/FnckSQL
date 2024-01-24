pub mod aggregate;
pub mod alter_table;
pub mod copy_from_file;
pub mod copy_to_file;
pub mod create_table;
pub mod delete;
pub mod drop_table;
pub mod filter;
pub mod insert;
pub mod join;
pub mod limit;
pub mod project;
pub mod scan;
pub mod show;
pub mod sort;
pub mod truncate;
pub mod update;
pub mod values;

use crate::catalog::ColumnRef;
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::planner::operator::copy_to_file::CopyToFileOperator;
use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::operator::drop_table::DropTableOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::show::ShowTablesOperator;
use crate::planner::operator::truncate::TruncateOperator;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::types::index::IndexInfo;
use itertools::Itertools;

use self::{
    aggregate::AggregateOperator, alter_table::add_column::AddColumnOperator,
    filter::FilterOperator, join::JoinOperator, limit::LimitOperator, project::ProjectOperator,
    scan::ScanOperator, sort::SortOperator,
};

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum Operator {
    // DQL
    Dummy,
    Aggregate(AggregateOperator),
    Filter(FilterOperator),
    Join(JoinOperator),
    Project(ProjectOperator),
    Scan(ScanOperator),
    Sort(SortOperator),
    Limit(LimitOperator),
    Values(ValuesOperator),
    // DML
    Insert(InsertOperator),
    Update(UpdateOperator),
    Delete(DeleteOperator),
    // DDL
    AddColumn(AddColumnOperator),
    DropColumn(DropColumnOperator),
    CreateTable(CreateTableOperator),
    DropTable(DropTableOperator),
    Truncate(TruncateOperator),
    // Show
    Show(ShowTablesOperator),
    // Copy
    CopyFromFile(CopyFromFileOperator),
    CopyToFile(CopyToFileOperator),
}

#[derive(Debug, Clone)]
pub enum PhysicalOption {
    Dummy,
    SimpleAggregate,
    HashAggregate,
    Filter,
    HashJoin,
    Project,
    SeqScan,
    IndexScan(IndexInfo),
    RadixSort,
    // NormalSort,
    Limit,
    Values,
    Insert,
    Update,
    Delete,
    AddColumn,
    DropColumn,
    CreateTable,
    DropTable,
    Truncate,
    Show,
    CopyFromFile,
    CopyToFile,
}

impl Operator {
    pub fn referenced_columns(&self, only_column_ref: bool) -> Vec<ColumnRef> {
        match self {
            Operator::Aggregate(op) => op
                .agg_calls
                .iter()
                .chain(op.groupby_exprs.iter())
                .flat_map(|expr| expr.referenced_columns(only_column_ref))
                .collect_vec(),
            Operator::Filter(op) => op.predicate.referenced_columns(only_column_ref),
            Operator::Join(op) => {
                let mut exprs = Vec::new();

                if let JoinCondition::On { on, filter } = &op.on {
                    for (left_expr, right_expr) in on {
                        exprs.append(&mut left_expr.referenced_columns(only_column_ref));
                        exprs.append(&mut right_expr.referenced_columns(only_column_ref));
                    }

                    if let Some(filter_expr) = filter {
                        exprs.append(&mut filter_expr.referenced_columns(only_column_ref));
                    }
                }
                exprs
            }
            Operator::Project(op) => op
                .exprs
                .iter()
                .flat_map(|expr| expr.referenced_columns(only_column_ref))
                .collect_vec(),
            Operator::Scan(op) => op
                .columns
                .iter()
                .flat_map(|expr| expr.referenced_columns(only_column_ref))
                .collect_vec(),
            Operator::Sort(op) => op
                .sort_fields
                .iter()
                .map(|field| &field.expr)
                .flat_map(|expr| expr.referenced_columns(only_column_ref))
                .collect_vec(),
            Operator::Values(op) => op.columns.clone(),
            _ => vec![],
        }
    }
}
