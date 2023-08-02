use anyhow::Result;
use itertools::Itertools;
use sqlparser::ast::{Expr, OrderByExpr};

use crate::{
    expression::ScalarExpression,
    planner::{
        operator::{aggregate::AggregateOperator, sort::SortField},
    },
};
use crate::planner::LogicalPlan;

use super::Binder;

impl Binder {
    pub fn bind_aggregate(
        &mut self,
        children: LogicalPlan,
        agg_calls: Vec<ScalarExpression>,
        groupby_exprs: Vec<ScalarExpression>,
    ) -> LogicalPlan {
        AggregateOperator::new(children, agg_calls, groupby_exprs)
    }

    pub fn extract_select_aggregate(
        &mut self,
        select_items: &mut [ScalarExpression],
    ) -> Result<()> {
        for column in select_items {
            self.visit_column_agg_expr(column);
        }
        Ok(())
    }

    pub fn extract_group_by_aggregate(
        &mut self,
        select_list: &mut [ScalarExpression],
        groupby: &[Expr],
    ) -> Result<()> {
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
    ) -> Result<(Option<ScalarExpression>, Option<Vec<SortField>>)> {
        // Extract having expression.
        let return_having = if let Some(having) = having {
            let mut having = self.bind_expr(having)?;
            self.visit_column_agg_expr(&mut having);
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
                self.visit_column_agg_expr(&mut expr);

                return_orderby.push(SortField::new(
                    expr,
                    asc.map_or(true, |asc| !asc),
                    nulls_first.map_or(false, |first| first),
                ));
            }
            Some(return_orderby)
        } else {
            None
        };
        Ok((return_having, return_orderby))
    }

    fn visit_column_agg_expr(&mut self, expr: &mut ScalarExpression) {
        match expr {
            ScalarExpression::AggCall {
                ty: return_type, ..
            } => {
                let index = if self.context.agg_calls.len() == 0 {
                    0
                } else {
                    self.context.agg_calls.len() + 1
                };
                let input_ref = ScalarExpression::InputRef {
                    index,
                    ty: return_type.clone(),
                };
                match std::mem::replace(expr, input_ref) {
                    ScalarExpression::AggCall {
                        kind,
                        args,
                        ty: return_type,
                    } => {
                        self.context.agg_calls.push(ScalarExpression::AggCall {
                            kind,
                            args,
                            ty: return_type,
                        });
                    }
                    _ => unreachable!(),
                }
            }

            ScalarExpression::TypeCast { expr, .. } => self.visit_column_agg_expr(expr),
            ScalarExpression::IsNull { expr } => self.visit_column_agg_expr(expr),
            ScalarExpression::Unary { expr, .. } => self.visit_column_agg_expr(expr),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => {
                self.visit_column_agg_expr(left_expr);
                self.visit_column_agg_expr(right_expr);
            }

            ScalarExpression::Constant(_)
            | ScalarExpression::ColumnRef { .. }
            | ScalarExpression::InputRef { .. }
            | ScalarExpression::Alias { .. } => {}
        }
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
    ) -> Result<()> {
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

        for column in select_items {
            let expr = &column;
            if expr.has_agg_call() {
                continue;
            }

            if !group_raw_exprs.iter().contains(expr) {
                return Err(anyhow::Error::msg(format!(
                    "{} must appear in the GROUP BY clause or be used in an aggregate function",
                    expr
                )));
            }
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
                let index = if self.context.group_by_exprs.len() == 0 {
                    0
                } else {
                    self.context.group_by_exprs.len() + 1
                };

                let mut select_item = &mut select_list[i];
                let return_type = select_item.return_type();
                self.context.group_by_exprs.push(std::mem::replace(
                    &mut select_item,
                    ScalarExpression::InputRef {
                        index,
                        ty: return_type,
                    },
                ));
                return;
            }
        }

        if let Some(i) = select_list.iter().position(|column| column == expr) {
            let expr = &mut select_list[i];
            match expr {
                ScalarExpression::Constant(_) | ScalarExpression::ColumnRef { .. } => {
                    self.context.group_by_exprs.push(expr.clone())
                }
                _ => {
                    let index = if self.context.group_by_exprs.len() == 0 {
                        0
                    } else {
                        self.context.group_by_exprs.len() + 1
                    };
                    self.context.group_by_exprs.push(std::mem::replace(
                        expr,
                        ScalarExpression::InputRef {
                            index,
                            ty: expr.return_type(),
                        },
                    ))
                }
            }
        }
    }

    /// Validate having or orderby clause is valid, if SQL has group by clause.
    pub fn validate_having_orderby(&self, expr: &ScalarExpression) -> Result<()> {
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

                Err(anyhow::Error::msg(format!(
                            "column {} must appear in the GROUP BY clause or be used in an aggregate function",
                            expr
                        )))
            }
            ScalarExpression::ColumnRef { .. } | ScalarExpression::Alias { .. } => {
                if self.context.group_by_exprs.contains(expr) {
                    return Ok(());
                }

                Err(anyhow::Error::msg(format!(
                            "column {} must appear in the GROUP BY clause or be used in an aggregate function",
                            expr
                        )))
            }

            ScalarExpression::TypeCast { expr, .. } => self.validate_having_orderby(expr),
            ScalarExpression::IsNull { expr } => self.validate_having_orderby(expr),
            ScalarExpression::Unary { expr, .. } => self.validate_having_orderby(expr),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => {
                self.validate_having_orderby(left_expr)?;
                self.validate_having_orderby(right_expr)?;
                Ok(())
            }

            ScalarExpression::Constant(_) | ScalarExpression::InputRef { .. } => Ok(()),
        }
    }
}
