pub mod aggregate;
pub mod create_table;
pub mod filter;
pub mod join;
pub mod limit;
pub mod project;
pub mod scan;
pub mod sort;
pub mod insert;
pub mod values;
pub mod update;

use itertools::Itertools;
use crate::catalog::ColumnRef;
use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::operator::values::ValuesOperator;

use self::{
    aggregate::AggregateOperator, filter::FilterOperator, join::JoinOperator, limit::LimitOperator,
    project::ProjectOperator, scan::ScanOperator, sort::SortOperator,
};

#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    Dummy,
    Aggregate(AggregateOperator),
    Filter(FilterOperator),
    Join(JoinOperator),
    Project(ProjectOperator),
    Scan(ScanOperator),
    Sort(SortOperator),
    Limit(LimitOperator),
    Insert(InsertOperator),
    Update(UpdateOperator),
    Values(ValuesOperator),
    CreateTable(CreateTableOperator)
}

impl Operator {
    pub fn referenced_columns(&self) -> Vec<ColumnRef> {
        match self {
            Operator::Aggregate(op) => {
                op.agg_calls
                    .iter()
                    .chain(op.groupby_exprs.iter())
                    .flat_map(|expr| expr.referenced_columns())
                    .collect_vec()
            }
            Operator::Filter(op) => {
                op.predicate.referenced_columns()
            }
            Operator::Join(op) => {
                let mut exprs = Vec::new();

                if let JoinCondition::On { on, filter } = &op.on {
                    for (left_expr, right_expr) in on {
                        exprs.append(&mut left_expr.referenced_columns());
                        exprs.append(&mut right_expr.referenced_columns());
                    }

                    if let Some(filter_expr) = filter {
                        exprs.append(&mut filter_expr.referenced_columns());
                    }
                }

                exprs
            }
            Operator::Project(op) => {
                op.columns
                    .iter()
                    .flat_map(|expr| expr.referenced_columns())
                    .collect_vec()
            }
            Operator::Scan(op) => {
                op.sort_fields
                    .iter()
                    .map(|field| &field.expr)
                    .chain(op.columns.iter())
                    .chain(op.pre_where.iter())
                    .flat_map(|expr| expr.referenced_columns())
                    .collect_vec()
            }
            Operator::Sort(op) => {
                op.sort_fields
                    .iter()
                    .map(|field| &field.expr)
                    .flat_map(|expr| expr.referenced_columns())
                    .collect_vec()
            }
            Operator::Values(op) => {
                op.columns.clone()
            }
            Operator::CreateTable(op) => {
                op.columns.clone()
            }
            _ => vec![],

        }
    }
}
