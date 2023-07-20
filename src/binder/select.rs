use std::{borrow::Borrow, sync::Arc};

use crate::{
    catalog::ColumnRefId,
    expression::ScalarExpression,
    planner::{
        logical_select_plan::LogicalSelectPlan,
        operator::{
            filter::FilterOperator, join::JoinOperator as LJoinOperator, limit::LimitOperator,
            project::ProjectOperator, Operator,
        },
        operator::{join::JoinType, scan::ScanOperator},
    },
    types::value::DataValue,
};

use super::Binder;

use crate::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use anyhow::Result;
use itertools::Itertools;
use sqlparser::ast::{
    Expr, Ident, Join, JoinConstraint, JoinOperator, Offset, OrderByExpr, Query, Select,
    SelectItem, SetExpr, TableFactor, TableWithJoins,
};

impl Binder {
    pub(crate) fn bind_query(&mut self, query: &Query) -> Result<LogicalSelectPlan> {
        if let Some(_with) = &query.with {
            // TODO support with clause.
        }

        let mut plan = match query.body.borrow() {
            SetExpr::Select(select) => self.bind_select(select, &query.order_by),
            SetExpr::Query(query) => self.bind_query(query),
            _ => unimplemented!(),
        }?;

        let limit = &query.limit;
        let offset = &query.offset;

        if limit.is_some() || offset.is_some() {
            plan = self.bind_limit(plan, limit, offset)?;
        }

        Ok(plan)
    }

    fn bind_select(
        &mut self,
        select: &Select,
        orderby: &[OrderByExpr],
    ) -> Result<LogicalSelectPlan> {
        let mut plan = self.bind_table_ref(&select.from)?;

        // Resolve scalar function call.
        // TODO support SRF(Set-Returning Function).

        let mut select_list = self.normalize_select_item(&select.projection)?;

        if let Some(predicate) = &select.selection {
            plan = self.bind_where(plan, predicate)?;
        }

        self.extract_select_aggregate(&mut select_list)?;

        if !select.group_by.is_empty() {
            self.extract_group_by_aggregate(&mut select_list, &select.group_by)?;
        }

        let mut having_orderby = (None, None);

        if select.having.is_some() || !orderby.is_empty() {
            having_orderby = self.extract_having_orderby_aggregate(&select.having, orderby)?;
        }

        if !self.context.agg_calls.is_empty() || !self.context.group_by_exprs.is_empty() {
            plan = self.bind_aggregate(
                plan,
                self.context.agg_calls.clone(),
                self.context.group_by_exprs.clone(),
            );
        }

        if let Some(having) = having_orderby.0 {
            plan = self.bind_having(plan, having)?;
        }

        // if select.distinct {
        //     plan = self.bind_distinct(plan, select_list.clone())?;
        // }

        // // if let Some(orderby) = having_orderby.1 {
        // //     plan = self.bind_sort(plan, orderby)?;
        // // }

        plan = self.bind_project(plan, select_list)?;
        Ok(plan)
    }

    fn bind_table_ref(&mut self, from: &[TableWithJoins]) -> Result<LogicalSelectPlan> {
        assert!(from.len() < 2, "not support yet.");
        if from.is_empty() {
            return Ok(LogicalSelectPlan {
                operator: Arc::new(Operator::Dummy),
                children: vec![],
            });
        }

        let TableWithJoins { relation, joins } = &from[0];

        let mut plan = self.bind_single_table_ref(relation)?;

        if !joins.is_empty() {
            for join in joins {
                plan = self.bind_join(plan, join)?;
            }
        }
        Ok(plan)
    }

    fn bind_single_table_ref(&mut self, table: &TableFactor) -> Result<LogicalSelectPlan> {
        let plan = match table {
            TableFactor::Table { name, alias, .. } => {
                let obj_name = name
                    .0
                    .iter()
                    .map(|ident| Ident::new(ident.value.to_lowercase()))
                    .collect_vec();

                let (_database, _schema, mut table): (&str, &str, &str) = match obj_name.as_slice()
                {
                    [table] => (DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, &table.value),
                    [schema, table] => (DEFAULT_DATABASE_NAME, &schema.value, &table.value),
                    [database, schema, table] => (&database.value, &schema.value, &table.value),
                    _ => return Err(anyhow::Error::msg(format!("table {:?}", obj_name))),
                };
                if let Some(alias) = alias {
                    table = &alias.name.value;
                }

                if self.context.bind_table.contains_key(table) {
                    return Err(anyhow::Error::msg(format!(
                        "bind duplicated table {}",
                        table
                    )));
                }

                let table_ref_id = self
                    .context
                    .catalog
                    .get_table_id_by_name(table)
                    .ok_or_else(|| anyhow::Error::msg(format!("bind table {}", table)))?;

                self.context.bind_table.insert(table.into(), table_ref_id);

                ScanOperator::new(table_ref_id)
            }
            _ => unimplemented!(),
        };

        Ok(plan)
    }

    /// Normalize select item.
    ///
    /// - Qualified name, e.g. `SELECT t.a FROM t`
    /// - Qualified name with wildcard, e.g. `SELECT t.* FROM t,t1`
    /// - Scalar expression or aggregate expression, e.g. `SELECT COUNT(*) + 1 AS count FROM t`
    ///  
    fn normalize_select_item(&mut self, items: &[SelectItem]) -> Result<Vec<ScalarExpression>> {
        let mut select_items = vec![];

        for item in items.iter().enumerate() {
            match item.1 {
                SelectItem::UnnamedExpr(expr) => select_items.push(self.bind_expr(expr)?),
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr = self.bind_expr(expr)?;
                    let alias_name = alias.to_string();

                    self.context.add_alias(alias_name.clone(), expr.clone());

                    select_items.push(ScalarExpression::Alias {
                        expr: Box::new(expr),
                        alias: alias_name,
                    });
                }
                SelectItem::Wildcard(_) => {
                    select_items.extend_from_slice(self.bind_all_column_refs()?.as_slice());
                }

                _ => todo!("bind select list"),
            };
        }

        Ok(select_items)
    }

    fn bind_all_column_refs(&mut self) -> Result<Vec<ScalarExpression>> {
        let mut exprs = vec![];
        for ref_id in self.context.bind_table.values().cloned().collect_vec() {
            let table = self.context.catalog.get_table(ref_id).unwrap();
            for (col_id, col) in &table.get_all_columns() {
                let column_ref_id = ColumnRefId::from_table(ref_id, *col_id);
                // self.record_regular_table_column(
                //     &table.name(),
                //     col.name(),
                //     *col_id,
                //     col.desc().clone(),
                // );
                let expr = ScalarExpression::ColumnRef {
                    column_ref_id,
                    primary_key: col.desc.is_primary(),
                    desc: col.desc.clone(),
                };
                exprs.push(expr);
            }
        }

        Ok(exprs)
    }

    fn bind_join(&mut self, left: LogicalSelectPlan, join: &Join) -> Result<LogicalSelectPlan> {
        let Join {
            relation,
            join_operator,
        } = join;

        let right = self.bind_single_table_ref(relation)?;

        let join_type = match join_operator {
            JoinOperator::Inner(constraint) => (JoinType::Inner, Some(constraint)),
            JoinOperator::LeftOuter(constraint) => (JoinType::LeftOuter, Some(constraint)),
            JoinOperator::RightOuter(constraint) => (JoinType::RightOuter, Some(constraint)),
            JoinOperator::FullOuter(constraint) => (JoinType::FullOuter, Some(constraint)),
            JoinOperator::CrossJoin => (JoinType::Cross, None),
            JoinOperator::LeftSemi(constraint) => (JoinType::LeftSemi, Some(constraint)),
            JoinOperator::RightSemi(constraint) => (JoinType::RightSemi, Some(constraint)),
            JoinOperator::LeftAnti(constraint) => (JoinType::LeftAnti, Some(constraint)),
            JoinOperator::RightAnti(constraint) => (JoinType::RightAnti, Some(constraint)),
            _ => unimplemented!(),
        };

        let on = match join_type.1 {
            Some(constraint) => match constraint {
                JoinConstraint::On(expr) => Some(self.bind_expr(expr)?),
                _ => unimplemented!(),
            },
            None => None,
        };
        Ok(LJoinOperator::new(left, right, on, join_type.0))
    }

    fn bind_where(
        &mut self,
        children: LogicalSelectPlan,
        predicate: &Expr,
    ) -> Result<LogicalSelectPlan> {
        Ok(FilterOperator::new(
            self.bind_expr(predicate)?,
            children,
            false,
        ))
    }

    fn bind_having(
        &mut self,
        children: LogicalSelectPlan,
        having: ScalarExpression,
    ) -> Result<LogicalSelectPlan> {
        self.validate_having_orderby(&having)?;
        Ok(FilterOperator::new(having, children, true))
    }

    fn bind_project(
        &mut self,
        children: LogicalSelectPlan,
        select_list: Vec<ScalarExpression>,
    ) -> Result<LogicalSelectPlan> {
        Ok(ProjectOperator::new(select_list, children))
    }

    fn bind_limit(
        &mut self,
        children: LogicalSelectPlan,
        limit_expr: &Option<Expr>,
        offset_expr: &Option<Offset>,
    ) -> Result<LogicalSelectPlan> {
        let mut limit = 0;
        let mut offset = 0;
        if let Some(expr) = limit_expr {
            let expr = self.bind_expr(expr)?;
            match expr {
                ScalarExpression::Constant(dv) => match dv {
                    DataValue::Int32(Some(v)) if v > 0 => limit = v as usize,
                    DataValue::Int64(Some(v)) if v > 0 => limit = v as usize,
                    _ => return Err(anyhow::Error::msg("invalid limit expression.".to_owned())),
                },
                _ => return Err(anyhow::Error::msg("invalid limit expression.".to_owned())),
            }
        }

        if let Some(expr) = offset_expr {
            let expr = self.bind_expr(&expr.value)?;
            match expr {
                ScalarExpression::Constant(dv) => match dv {
                    DataValue::Int32(Some(v)) if v > 0 => offset = v as usize,
                    DataValue::Int64(Some(v)) if v > 0 => offset = v as usize,
                    _ => return Err(anyhow::Error::msg("invalid limit expression.".to_owned())),
                },
                _ => return Err(anyhow::Error::msg("invalid offset expression.".to_owned())),
            }
        }

        // TODO: validate limit and offset is correct use statistic.

        Ok(LimitOperator::new(offset, limit, children))
    }
}
