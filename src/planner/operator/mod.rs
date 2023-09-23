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
pub mod delete;
pub mod drop_table;
pub mod truncate;
pub mod show;

use itertools::Itertools;
use crate::catalog::ColumnRef;
use crate::expression::ScalarExpression;
use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::operator::drop_table::DropTableOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::show::ShowTablesOperator;
use crate::planner::operator::truncate::TruncateOperator;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::operator::values::ValuesOperator;

use self::{
    aggregate::AggregateOperator, filter::FilterOperator, join::JoinOperator, limit::LimitOperator,
    project::ProjectOperator, scan::ScanOperator, sort::SortOperator,
};

#[derive(Debug, PartialEq, Clone)]
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
    CreateTable(CreateTableOperator),
    DropTable(DropTableOperator),
    Truncate(TruncateOperator),
    // Show
    Show(ShowTablesOperator),
}

impl Operator {
    pub fn project_input_refs(&self) -> Vec<ScalarExpression> {
        match self {
            Operator::Project(op) => {
                op.columns
                    .iter()
                    .map(ScalarExpression::unpack_alias)
                    .filter(|expr| matches!(expr, ScalarExpression::InputRef { .. }))
                    .cloned()
                    .collect_vec()
            }
            _ => vec![],
        }
    }

    pub fn agg_mapping_col_refs(&self, input_refs: &[ScalarExpression]) -> Vec<ColumnRef> {
        match self {
            Operator::Aggregate(AggregateOperator { agg_calls, .. }) => {
                input_refs.iter()
                    .filter_map(|expr| {
                        if let ScalarExpression::InputRef { index, .. } = expr {
                            Some(agg_calls[*index].clone())
                        } else {
                            None
                        }
                    })
                    .map(|expr| expr.referenced_columns())
                    .flatten()
                    .collect_vec()
            }
            _ => vec![],
        }
    }

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
                op.columns.iter()
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
            _ => vec![],
        }
    }
}
