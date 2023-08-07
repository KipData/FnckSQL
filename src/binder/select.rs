use std::borrow::Borrow;

use crate::{
    expression::ScalarExpression,
    planner::{
        operator::{
            filter::FilterOperator, join::JoinOperator as LJoinOperator, limit::LimitOperator,
            project::ProjectOperator, Operator,
        },
        operator::{join::JoinType, scan::ScanOperator},
    },
    types::value::DataValue,
};

use super::Binder;

use crate::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, TableCatalog};
use anyhow::Result;
use itertools::Itertools;
use sqlparser::ast;
use sqlparser::ast::{
    Expr, Ident, Join, JoinConstraint, JoinOperator, Offset, OrderByExpr, Query, Select,
    SelectItem, SetExpr, TableFactor, TableWithJoins,
};
use crate::expression::BinaryOperator;
use crate::planner::LogicalPlan;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::types::{LogicalType, TableId};

impl Binder {
    pub(crate) fn bind_query(&mut self, query: &Query) -> Result<LogicalPlan> {
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
    ) -> Result<LogicalPlan> {
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

        if let Some(orderby) = having_orderby.1 {
            plan = self.bind_sort(plan, orderby);
        }

        plan = self.bind_project(plan, select_list);

        Ok(plan)
    }

    fn bind_table_ref(&mut self, from: &[TableWithJoins]) -> Result<LogicalPlan> {
        assert!(from.len() < 2, "not support yet.");
        if from.is_empty() {
            return Ok(LogicalPlan {
                operator: Operator::Dummy,
                childrens: vec![],
            });
        }

        let TableWithJoins { relation, joins } = &from[0];

        let (left_id, mut plan) = self.bind_single_table_ref(relation, None)?;

        if !joins.is_empty() {
            for join in joins {
                plan = self.bind_join(left_id, plan, join)?;
            }
        }
        Ok(plan)
    }

    fn bind_single_table_ref(&mut self, table: &TableFactor, joint_type: Option<JoinType>) -> Result<(TableId, LogicalPlan)> {
        let plan_with_id = match table {
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

                self.context.bind_table.insert(table.into(), (table_ref_id, joint_type));

                (table_ref_id, ScanOperator::new(table_ref_id))
            }
            _ => unimplemented!(),
        };

        Ok(plan_with_id)
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
        for (table_id, _) in self.context.bind_table.values().cloned() {
            let table = self.context.catalog.get_table(&table_id).unwrap();
            for (_, col) in table.all_columns() {
                exprs.push(ScalarExpression::ColumnRef(col.clone()));
            }
        }

        Ok(exprs)
    }

    fn bind_join(&mut self, left_id: TableId, left: LogicalPlan, join: &Join) -> Result<LogicalPlan> {
        let Join {
            relation,
            join_operator,
        } = join;

        let (join_type, joint_condition) = match join_operator {
            JoinOperator::Inner(constraint) => (JoinType::Inner, Some(constraint)),
            JoinOperator::LeftOuter(constraint) => (JoinType::Left, Some(constraint)),
            JoinOperator::RightOuter(constraint) => (JoinType::Right, Some(constraint)),
            JoinOperator::FullOuter(constraint) => (JoinType::Full, Some(constraint)),
            JoinOperator::CrossJoin => (JoinType::Cross, None),
            _ => unimplemented!(),
        };

        let (right_id, right) = self.bind_single_table_ref(relation, Some(join_type))?;

        let left_table = self.context.catalog
            .get_table(&left_id)
            .cloned()
            .expect("Left table not found");
        let right_table = self.context.catalog
            .get_table(&right_id)
            .cloned()
            .expect("Right table not found");

        let on = match joint_condition {
            Some(constraint) => self.bind_join_constraint(
                &left_table,
                &right_table,
                constraint
            )?,
            None => JoinCondition::None,
        };

        Ok(LJoinOperator::new(left, right, on, join_type))
    }

    fn bind_where(
        &mut self,
        children: LogicalPlan,
        predicate: &Expr,
    ) -> Result<LogicalPlan> {
        Ok(FilterOperator::new(
            self.bind_expr(predicate)?,
            children,
            false,
        ))
    }

    fn bind_having(
        &mut self,
        children: LogicalPlan,
        having: ScalarExpression,
    ) -> Result<LogicalPlan> {
        self.validate_having_orderby(&having)?;
        Ok(FilterOperator::new(having, children, true))
    }

    fn bind_project(
        &mut self,
        children: LogicalPlan,
        select_list: Vec<ScalarExpression>,
    ) -> LogicalPlan {
        LogicalPlan {
            operator: Operator::Project(ProjectOperator {
                columns: select_list,
            }),
            childrens: vec![children],
        }
    }

    fn bind_sort(
        &mut self,
        children: LogicalPlan,
        sort_fields: Vec<SortField>,
    ) -> LogicalPlan {
        LogicalPlan {
            operator: Operator::Sort(SortOperator {
                sort_fields,
                limit: None,
            }),
            childrens: vec![children],
        }
    }

    fn bind_limit(
        &mut self,
        children: LogicalPlan,
        limit_expr: &Option<Expr>,
        offset_expr: &Option<Offset>,
    ) -> Result<LogicalPlan> {
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

    fn bind_join_constraint(
        &mut self,
        left_table: &TableCatalog,
        right_table: &TableCatalog,
        constraint: &JoinConstraint,
    ) -> Result<JoinCondition> {
        match constraint {
            JoinConstraint::On(expr) => {
                // left and right columns that match equi-join pattern
                let mut on_keys: Vec<(ScalarExpression, ScalarExpression)> = vec![];
                // expression that didn't match equi-join pattern
                let mut filter = vec![];

                self.extract_join_keys(expr, &mut on_keys, &mut filter, left_table, right_table)?;

                // combine multiple filter exprs into one BinaryExpr
                let join_filter = filter
                    .into_iter()
                    .reduce(|acc, expr| ScalarExpression::Binary {
                        op: BinaryOperator::And,
                        left_expr: Box::new(acc),
                        right_expr: Box::new(expr),
                        ty: LogicalType::Boolean,
                    });
                // TODO: handle cross join if on_keys is empty
                Ok(JoinCondition::On {
                    on: on_keys,
                    filter: join_filter,
                })
            }
            _ => unimplemented!("not supported join constraint {:?}", constraint),
        }
    }

    /// for sqlrs
    /// original idea from datafusion planner.rs
    /// Extracts equijoin ON condition be a single Eq or multiple conjunctive Eqs
    /// Filters matching this pattern are added to `accum`
    /// Filters that don't match this pattern are added to `accum_filter`
    /// Examples:
    /// ```text
    /// foo = bar => accum=[(foo, bar)] accum_filter=[]
    /// foo = bar AND bar = baz => accum=[(foo, bar), (bar, baz)] accum_filter=[]
    /// foo = bar AND baz > 1 => accum=[(foo, bar)] accum_filter=[baz > 1]
    /// ```
    fn extract_join_keys(
        &mut self,
        expr: &Expr,
        accum: &mut Vec<(ScalarExpression, ScalarExpression)>,
        accum_filter: &mut Vec<ScalarExpression>,
        left_schema: &TableCatalog,
        right_schema: &TableCatalog,
    ) -> Result<()> {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                ast::BinaryOperator::Eq => {
                    let left = self.bind_expr(left)?;
                    let right = self.bind_expr(right)?;

                    match (&left, &right) {
                        // example: foo = bar
                        (ScalarExpression::ColumnRef(l), ScalarExpression::ColumnRef(r)) => {
                            // reorder left and right joins keys to pattern: (left, right)
                            if left_schema.contains_column(&l.name)
                                && right_schema.contains_column(&r.name)
                            {
                                accum.push((left, right));
                            } else if left_schema.contains_column(&r.name)
                                && right_schema.contains_column(&l.name)
                            {
                                accum.push((right, left));
                            } else {
                                accum_filter.push(self.bind_expr(expr)?);
                            }
                        }
                        // example: baz = 1
                        _other => {
                            accum_filter.push(self.bind_expr(expr)?);
                        }
                    }
                }
                ast::BinaryOperator::And => {
                    // example: foo = bar AND baz > 1
                    if let Expr::BinaryOp { left, op: _, right } = expr {
                        self.extract_join_keys(
                            left,
                            accum,
                            accum_filter,
                            left_schema,
                            right_schema,
                        )?;
                        self.extract_join_keys(
                            right,
                            accum,
                            accum_filter,
                            left_schema,
                            right_schema,
                        )?;
                    }
                }
                _other => {
                    // example: baz > 1
                    accum_filter.push(self.bind_expr(expr)?);
                }
            },
            _other => {
                // example: baz in (xxx), something else will convert to filter logic
                accum_filter.push(self.bind_expr(expr)?);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::BinderContext;
    use crate::catalog::{ColumnCatalog, ColumnDesc, RootCatalog};
    use crate::planner::LogicalPlan;
    use crate::types::LogicalType::Integer;

    fn test_root_catalog() -> Result<RootCatalog> {
        let mut root = RootCatalog::new();

        let cols_t1 = vec![
            ColumnCatalog::new("c1".to_string(), false, ColumnDesc::new(Integer, true)),
            ColumnCatalog::new("c2".to_string(), false, ColumnDesc::new(Integer, false)),
        ];
        let _ = root.add_table("t1".to_string(), cols_t1)?;

        let cols_t2 = vec![
            ColumnCatalog::new("c3".to_string(), false, ColumnDesc::new(Integer, true)),
            ColumnCatalog::new("c4".to_string(), false, ColumnDesc::new(Integer, false)),
        ];
        let _ = root.add_table("t2".to_string(), cols_t2)?;
        Ok(root)
    }

    fn select_sql_run(sql: &str) -> Result<LogicalPlan> {
        let root = test_root_catalog()?;

        let binder = Binder::new(BinderContext::new(root));
        let stmt = crate::parser::parse_sql(sql).unwrap();

        Ok(binder.bind(&stmt[0])?.0)
    }

    #[test]
    fn test_select_bind() -> Result<()> {
        let plan_1 = select_sql_run("select * from t1")?;
        println!(
            "just_col:\n {:#?}",
            plan_1
        );
        let plan_2 = select_sql_run("select t1.c1, t1.c2 from t1")?;
        println!(
            "table_with_col:\n {:#?}",
            plan_2
        );
        let plan_3 = select_sql_run("select t1.c1, t1.c2 from t1 where c1 > 2")?;
        println!(
            "table_with_col_and_c1_compare_constant:\n {:#?}",
            plan_3
        );
        let plan_4 = select_sql_run("select t1.c1, t1.c2 from t1 where c1 > c2")?;
        println!(
            "table_with_col_and_c1_compare_c2:\n {:#?}",
           plan_4
        );
        let plan_5 = select_sql_run("select avg(t1.c1) from t1")?;
        println!(
            "table_with_col_and_c1_avg:\n {:#?}",
            plan_5
        );
        let plan_6 = select_sql_run("select t1.c1, t1.c2 from t1 where (t1.c1 - t1.c2) > 1")?;
        println!(
            "table_with_col_nested:\n {:#?}",
            plan_6
        );

        let plan_7 = select_sql_run("select * from t1 limit 1")?;
        println!(
            "limit:\n {:#?}",
            plan_7
        );

        let plan_8 = select_sql_run("select * from t1 offset 2")?;
        println!(
            "offset:\n {:#?}",
            plan_8
        );

        let plan_9 = select_sql_run("select c1, c3 from t1 inner join t2 on c1 = c3 and c1 > 1")?;
        println!(
            "join:\n {:#?}",
            plan_9
        );

        Ok(())
    }
}