use std::mem;
use ahash::HashMap;
use crate::execution_v1::physical_plan::physical_create_table::PhysicalCreateTable;
use crate::execution_v1::physical_plan::physical_projection::PhysicalProjection;
use crate::execution_v1::physical_plan::physical_table_scan::PhysicalTableScan;
use crate::execution_v1::physical_plan::PhysicalPlan;
use crate::planner::operator::scan::ScanOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use anyhow::anyhow;
use anyhow::Result;
use itertools::Itertools;
use crate::binder::BinderContext;
use crate::execution_v1::physical_plan::physical_filter::PhysicalFilter;
use crate::execution_v1::physical_plan::physical_hash_join::PhysicalHashJoin;
use crate::execution_v1::physical_plan::physical_insert::PhysicalInsert;
use crate::execution_v1::physical_plan::physical_limit::PhysicalLimit;
use crate::execution_v1::physical_plan::physical_sort::PhysicalSort;
use crate::execution_v1::physical_plan::physical_values::PhysicalValues;
use crate::expression::ScalarExpression;
use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::operator::limit::LimitOperator;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::planner::operator::values::ValuesOperator;
use crate::types::ColumnId;

pub struct PhysicalPlanBuilder {
    column_index: HashMap<ColumnId, usize>
}

impl PhysicalPlanBuilder {
    pub fn new(context: BinderContext) -> Self {
        let mut pos = 0usize;
        let root = &context.catalog;
        let column_index = context.bind_table
            .iter()
            .filter_map(|(_, table_id)| {
                root.get_table(table_id)
                    .map(|table| {
                        table.all_columns()
                            .iter()
                            .map(|(col_id, _)| {
                                let next_pos = pos + 1;
                                (**col_id, mem::replace(&mut pos, next_pos))
                            })
                            .collect_vec()
                    })
            })
            .flatten()
            .collect();

        PhysicalPlanBuilder { column_index }
    }

    pub fn build_plan(&mut self, plan: &LogicalPlan) -> Result<PhysicalPlan> {
        match &plan.operator {
            Operator::Project(op) => self.build_physical_projection(plan, op),
            Operator::Scan(scan) => Ok(self.build_physical_scan(scan.clone())),
            Operator::Filter(op) => self.build_physical_filter(plan, op),
            Operator::CreateTable(op) => Ok(self.build_physical_create_table(op)),
            Operator::Insert(op) => self.build_insert(plan, op),
            Operator::Values(op) => Ok(Self::build_values(op)),
            Operator::Sort(op) => self.build_physical_sort(plan, op),
            Operator::Limit(op) => self.build_physical_limit(plan, op),
            Operator::Join(op) => self.build_physical_join(plan, op),
            _ => Err(anyhow!(format!(
                "Unsupported physical plan: {:?}",
                plan.operator
            ))),
        }
    }

    fn build_values(op: &ValuesOperator) -> PhysicalPlan {
        PhysicalPlan::Values(PhysicalValues { base: op.clone() })
    }

    fn build_insert(&mut self, plan: &LogicalPlan, op: &InsertOperator) -> Result<PhysicalPlan> {
        let input = self.build_plan(plan.child(0)?)?;

        Ok(PhysicalPlan::Insert(PhysicalInsert {
            table_name: op.table.clone(),
            input: Box::new(input),
        }))
    }

    fn build_physical_create_table(
        &mut self,
        op: &CreateTableOperator,
    ) -> PhysicalPlan {
        PhysicalPlan::CreateTable(
            PhysicalCreateTable {
                op: op.clone(),
            }
        )
    }

    fn build_physical_projection(&mut self, plan: &LogicalPlan, op: &ProjectOperator) -> Result<PhysicalPlan> {
        let input = self.build_plan(plan.child(0)?)?;

        let exprs = op.columns
            .iter()
            .map(|expr| self.rewriter_expr(expr))
            .collect_vec();

        Ok(PhysicalPlan::Projection(PhysicalProjection {
            exprs,
            input: Box::new(input),
        }))
    }

    fn build_physical_scan(&mut self, base: ScanOperator) -> PhysicalPlan {
        PhysicalPlan::TableScan(PhysicalTableScan { base })
    }

    fn build_physical_filter(&mut self, plan: &LogicalPlan, base: &FilterOperator) -> Result<PhysicalPlan> {
        let input = self.build_plan(plan.child(0)?)?;

        Ok(PhysicalPlan::Filter(PhysicalFilter {
            predicate: self.rewriter_expr(&base.predicate),
            input: Box::new(input),
        }))
    }

    fn build_physical_sort(&mut self, plan: &LogicalPlan, SortOperator { sort_fields, limit }: &SortOperator) -> Result<PhysicalPlan> {
        let input = self.build_plan(plan.child(0)?)?;

        let rewrite_sort_fields = sort_fields
            .into_iter()
            .map(|SortField{ expr, desc, nulls_first }| {
                SortField {
                    expr: self.rewriter_expr(expr),
                    desc: desc.clone(),
                    nulls_first: nulls_first.clone(),
                }
            })
            .collect_vec();

        Ok(PhysicalPlan::Sort(PhysicalSort {
            op: SortOperator {
                sort_fields: rewrite_sort_fields,
                limit: limit.clone(),
            },
            input: Box::new(input),
        }))
    }

    fn build_physical_limit(&mut self, plan: &LogicalPlan, base: &LimitOperator) -> Result<PhysicalPlan> {
        let input =self.build_plan(plan.child(0)?)?;

        Ok(PhysicalPlan::Limit(PhysicalLimit{
            op:base.clone(),
            input: Box::new(input),
        }))
    }

    fn build_physical_join(&mut self, plan: &LogicalPlan, JoinOperator{ on, join_type } : &JoinOperator) -> Result<PhysicalPlan> {
        let left_input = Box::new(self.build_plan(plan.child(0)?)?);
        let right_input = Box::new(self.build_plan(plan.child(1)?)?);

        let on =  if let JoinCondition::On { on, filter } = on {
            let rewrite_on = on.iter()
                .map(|(left_expr, right_expr)| {
                    (self.rewriter_expr(left_expr), self.rewriter_expr(right_expr))
                })
                .collect_vec();
            let filter = filter
                .as_ref()
                .map(|expr| self.rewriter_expr(expr));

            JoinCondition::On { on: rewrite_on, filter }
        } else {
            JoinCondition::None
        };

        if join_type == &JoinType::Cross {
            todo!()
        } else {
            Ok(PhysicalPlan::HashJoin(PhysicalHashJoin {
                op: JoinOperator {
                    on,
                    join_type: join_type.clone(),
                },
                left_input,
                right_input,
            }))
        }
    }

    fn rewriter_expr(&mut self, expr: &ScalarExpression) -> ScalarExpression {
        match expr {
            ScalarExpression::ColumnRef(col) => {
                ScalarExpression::InputRef {
                    // FIXME: remove unwrap
                    index: *self.column_index.get(&col.id.unwrap()).unwrap(),
                    ty: col.datatype().clone(),
                }
            }
            ScalarExpression::Alias { expr, alias } => {
                ScalarExpression::Alias {
                    expr: Box::new(self.rewriter_expr(expr)),
                    alias: alias.clone()
                }
            }
            ScalarExpression::TypeCast { expr, ty, is_try } => {
                ScalarExpression::TypeCast {
                    expr: Box::new(self.rewriter_expr(expr)),
                    ty: ty.clone(),
                    is_try: is_try.clone(),
                }
            }
            ScalarExpression::IsNull { expr } => {
                ScalarExpression::IsNull {
                    expr: Box::new(self.rewriter_expr(expr))
                }
            }
            ScalarExpression::Unary { op, expr, ty } => {
                ScalarExpression::Unary {
                    op: op.clone(),
                    expr: Box::new(self.rewriter_expr(expr)),
                    ty: ty.clone(),
                }
            }
            ScalarExpression::Binary { op, left_expr, right_expr, ty } => {
                ScalarExpression::Binary {
                    op: op.clone(),
                    left_expr: Box::new(self.rewriter_expr(left_expr)),
                    right_expr: Box::new(self.rewriter_expr(right_expr)),
                    ty: ty.clone(),
                }
            }
            ScalarExpression::AggCall { kind, args, ty } => {
                let rewrite_args = args
                    .into_iter()
                    .map(|expr| self.rewriter_expr(expr))
                    .collect_vec();

                ScalarExpression::AggCall {
                    kind: kind.clone(),
                    args: rewrite_args,
                    ty: ty.clone(),
                }
            }
            _ => expr.clone()
        }
    }
}