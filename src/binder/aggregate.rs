use ahash::RandomState;
use itertools::Itertools;
use sqlparser::ast::{Expr, OrderByExpr};
use std::collections::HashSet;

use crate::errors::DatabaseError;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::{
    expression::ScalarExpression,
    planner::operator::{aggregate::AggregateOperator, sort::SortField},
};

use super::Binder;

impl<'a, T: Transaction> Binder<'a, T> {
    pub fn bind_aggregate(
        &mut self,
        children: LogicalPlan,
        agg_calls: Vec<ScalarExpression>,
        groupby_exprs: Vec<ScalarExpression>,
    ) -> LogicalPlan {
        AggregateOperator::build(children, agg_calls, groupby_exprs)
    }

    pub fn extract_select_aggregate(
        &mut self,
        select_items: &mut [ScalarExpression],
    ) -> Result<(), DatabaseError> {
        for column in select_items {
            self.visit_column_agg_expr(column)?;
        }
        Ok(())
    }

    pub fn extract_group_by_aggregate(
        &mut self,
        select_list: &mut [ScalarExpression],
        groupby: &[Expr],
    ) -> Result<(), DatabaseError> {
        self.validate_groupby_illegal_column(select_list, groupby)?;

        for gb in groupby {
            let mut expr = self.bind_expr(gb)?;
            self.visit_group_by_expr(select_list, &mut expr);
        }
        Ok(())
    }

    pub fn extract_having_orderby_aggregate(
        &mut self,
        having: &Option<Expr>,
        orderbys: &[OrderByExpr],
    ) -> Result<(Option<ScalarExpression>, Option<Vec<SortField>>), DatabaseError> {
        // Extract having expression.
        let return_having = if let Some(having) = having {
            let mut having = self.bind_expr(having)?;
            self.visit_column_agg_expr(&mut having)?;

            Some(having)
        } else {
            None
        };

        // Extract orderby expression.
        let return_orderby = if !orderbys.is_empty() {
            let mut return_orderby = vec![];
            for orderby in orderbys {
                let OrderByExpr {
                    expr,
                    asc,
                    nulls_first,
                } = orderby;
                let mut expr = self.bind_expr(expr)?;
                self.visit_column_agg_expr(&mut expr)?;

                return_orderby.push(SortField::new(
                    expr,
                    asc.map_or(true, |asc| asc),
                    nulls_first.map_or(true, |first| first),
                ));
            }
            Some(return_orderby)
        } else {
            None
        };
        Ok((return_having, return_orderby))
    }

    fn visit_column_agg_expr(&mut self, expr: &mut ScalarExpression) -> Result<(), DatabaseError> {
        match expr {
            ScalarExpression::AggCall { .. } => {
                self.context.agg_calls.push(expr.clone());
            }
            ScalarExpression::TypeCast { expr, .. } => self.visit_column_agg_expr(expr)?,
            ScalarExpression::IsNull { expr, .. } => self.visit_column_agg_expr(expr)?,
            ScalarExpression::Unary { expr, .. } => self.visit_column_agg_expr(expr)?,
            ScalarExpression::Alias { expr, .. } => self.visit_column_agg_expr(expr)?,
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => {
                self.visit_column_agg_expr(left_expr)?;
                self.visit_column_agg_expr(right_expr)?;
            }
            ScalarExpression::In { expr, args, .. } => {
                self.visit_column_agg_expr(expr)?;
                for arg in args {
                    self.visit_column_agg_expr(arg)?;
                }
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                ..
            } => {
                self.visit_column_agg_expr(expr)?;
                self.visit_column_agg_expr(left_expr)?;
                self.visit_column_agg_expr(right_expr)?;
            }
            ScalarExpression::Constant(_) | ScalarExpression::ColumnRef { .. } => {}
        }

        Ok(())
    }

    /// Validate select exprs must appear in the GROUP BY clause or be used in
    /// an aggregate function.
    /// e.g. SELECT a,count(b) FROM t GROUP BY a. it's ok.
    ///      SELECT a,b FROM t GROUP BY a.        it's error.
    ///      SELECT a,count(b) FROM t GROUP BY b. it's error.
    fn validate_groupby_illegal_column(
        &mut self,
        select_items: &[ScalarExpression],
        groupby: &[Expr],
    ) -> Result<(), DatabaseError> {
        let mut group_raw_exprs = vec![];
        for expr in groupby {
            let expr = self.bind_expr(expr)?;

            if let ScalarExpression::Alias { alias, .. } = expr {
                let alias_expr = select_items.iter().find(|column| {
                    if let ScalarExpression::Alias {
                        alias: inner_alias, ..
                    } = &column
                    {
                        alias == *inner_alias
                    } else {
                        false
                    }
                });

                if let Some(inner_expr) = alias_expr {
                    group_raw_exprs.push(inner_expr.clone());
                }
            } else {
                group_raw_exprs.push(expr);
            }
        }
        let mut group_raw_set: HashSet<&ScalarExpression, RandomState> =
            HashSet::from_iter(group_raw_exprs.iter());

        for expr in select_items {
            if expr.has_agg_call() {
                continue;
            }
            group_raw_set.remove(expr);

            if !group_raw_exprs.iter().contains(expr) {
                return Err(DatabaseError::AggMiss(format!(
                    "`{}` must appear in the GROUP BY clause or be used in an aggregate function",
                    expr
                )));
            }
        }

        if !group_raw_set.is_empty() {
            return Err(DatabaseError::AggMiss(
                "In the GROUP BY clause the field must be in the select clause".to_string(),
            ));
        }

        Ok(())
    }

    fn visit_group_by_expr(
        &mut self,
        select_list: &mut [ScalarExpression],
        expr: &mut ScalarExpression,
    ) {
        if let ScalarExpression::Alias { alias, .. } = expr {
            if let Some(i) = select_list.iter().position(|inner_expr| {
                if let ScalarExpression::Alias {
                    alias: inner_alias, ..
                } = &inner_expr
                {
                    alias == inner_alias
                } else {
                    false
                }
            }) {
                self.context.group_by_exprs.push(select_list[i].clone());
                return;
            }
        }

        if let Some(i) = select_list.iter().position(|column| column == expr) {
            self.context.group_by_exprs.push(select_list[i].clone())
        }
    }

    /// Validate having or orderby clause is valid, if SQL has group by clause.
    pub fn validate_having_orderby(&self, expr: &ScalarExpression) -> Result<(), DatabaseError> {
        if self.context.group_by_exprs.is_empty() {
            return Ok(());
        }

        match expr {
            ScalarExpression::AggCall { .. } => {
                if self.context.group_by_exprs.contains(expr)
                    || self.context.agg_calls.contains(expr)
                {
                    return Ok(());
                }

                Err(DatabaseError::AggMiss(
                    format!(
                        "column {:?} must appear in the GROUP BY clause or be used in an aggregate function",
                        expr
                    )
                ))
            }
            ScalarExpression::ColumnRef { .. } | ScalarExpression::Alias { .. } => {
                if self.context.group_by_exprs.contains(expr) {
                    return Ok(());
                }
                if matches!(expr, ScalarExpression::Alias { .. }) {
                    return self.validate_having_orderby(expr.unpack_alias());
                }

                Err(DatabaseError::AggMiss(
                    format!(
                        "column {:?} must appear in the GROUP BY clause or be used in an aggregate function",
                        expr
                    )
                ))
            }

            ScalarExpression::TypeCast { expr, .. } => self.validate_having_orderby(expr),
            ScalarExpression::IsNull { expr, .. } => self.validate_having_orderby(expr),
            ScalarExpression::Unary { expr, .. } => self.validate_having_orderby(expr),
            ScalarExpression::In { expr, args, .. } => {
                self.validate_having_orderby(expr)?;
                for arg in args {
                    self.validate_having_orderby(arg)?;
                }
                Ok(())
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => {
                self.validate_having_orderby(left_expr)?;
                self.validate_having_orderby(right_expr)?;
                Ok(())
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                ..
            } => {
                self.validate_having_orderby(expr)?;
                self.validate_having_orderby(left_expr)?;
                self.validate_having_orderby(right_expr)?;
                Ok(())
            }
            ScalarExpression::Constant(_) => Ok(()),
        }
    }
}
