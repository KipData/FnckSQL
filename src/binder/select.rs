use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

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

use super::{lower_case_name, lower_ident, Binder, QueryBindStep};

use crate::catalog::{ColumnCatalog, ColumnSummary, TableName};
use crate::errors::DatabaseError;
use crate::execution::volcano::dql::join::joins_nullable;
use crate::expression::{AliasType, BinaryOperator};
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Schema;
use crate::types::LogicalType;
use itertools::Itertools;
use sqlparser::ast::{
    Distinct, Expr, Ident, Join, JoinConstraint, JoinOperator, Offset, OrderByExpr, Query, Select,
    SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins,
};

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_query(&mut self, query: &Query) -> Result<LogicalPlan, DatabaseError> {
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
    ) -> Result<LogicalPlan, DatabaseError> {
        let mut plan = self.bind_table_ref(&select.from)?;

        // Resolve scalar function call.
        // TODO support SRF(Set-Returning Function).

        let mut select_list = self.normalize_select_item(&select.projection)?;

        if let Some(predicate) = &select.selection {
            plan = self.bind_where(plan, predicate)?;
        }
        self.extract_select_join(&mut select_list);
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

        if let Some(Distinct::Distinct) = select.distinct {
            plan = self.bind_distinct(plan, select_list.clone());
        }

        if let Some(orderby) = having_orderby.1 {
            plan = self.bind_sort(plan, orderby);
        }

        plan = self.bind_project(plan, select_list)?;

        Ok(plan)
    }

    pub(crate) fn bind_table_ref(
        &mut self,
        from: &[TableWithJoins],
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::From);

        assert!(from.len() < 2, "not support yet.");
        if from.is_empty() {
            return Ok(LogicalPlan::new(Operator::Dummy, vec![]));
        }

        let TableWithJoins { relation, joins } = &from[0];

        let (left_name, mut plan) = self.bind_single_table_ref(relation, None)?;

        if !joins.is_empty() {
            let left_name = Self::unpack_name(left_name, true);

            for join in joins {
                plan = self.bind_join(left_name.clone(), plan, join)?;
            }
        }
        Ok(plan)
    }

    fn unpack_name(table_name: Option<TableName>, is_left: bool) -> TableName {
        let title = if is_left { "Left" } else { "Right" };
        table_name.unwrap_or_else(|| panic!("{}: Table is not named", title))
    }

    fn bind_single_table_ref(
        &mut self,
        table: &TableFactor,
        joint_type: Option<JoinType>,
    ) -> Result<(Option<TableName>, LogicalPlan), DatabaseError> {
        let plan_with_name = match table {
            TableFactor::Table { name, alias, .. } => {
                let table_name = lower_case_name(name)?;

                let (table, plan) =
                    self._bind_single_table_ref(joint_type, &table_name, alias.as_ref())?;
                (Some(table), plan)
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let plan = self.bind_query(subquery)?;
                let mut tables = plan.referenced_table();

                if let Some(TableAlias {
                    name,
                    columns: alias_column,
                }) = alias
                {
                    let table_alias = Arc::new(name.value.to_lowercase());

                    if tables.len() > 1 {
                        todo!("Implement virtual tables for multiple table aliases");
                    }
                    self.register_alias(alias_column, table_alias.to_string(), tables.remove(0))?;

                    (Some(table_alias), plan)
                } else {
                    ((tables.len() > 1).then(|| tables.pop()).flatten(), plan)
                }
            }
            _ => unimplemented!(),
        };

        Ok(plan_with_name)
    }

    fn register_alias(
        &mut self,
        alias_column: &[Ident],
        table_alias: String,
        table_name: TableName,
    ) -> Result<(), DatabaseError> {
        if !alias_column.is_empty() {
            let table = self
                .context
                .table(table_name.clone())
                .ok_or(DatabaseError::TableNotFound)?;

            if alias_column.len() != table.columns_len() {
                return Err(DatabaseError::MisMatch(
                    "Alias".to_string(),
                    "Columns".to_string(),
                ));
            }
            let aliases_with_columns = alias_column
                .iter()
                .map(lower_ident)
                .zip(table.columns().cloned())
                .collect_vec();

            for (alias, column) in aliases_with_columns {
                self.context
                    .add_alias(alias, ScalarExpression::ColumnRef(column));
            }
        }
        self.context.add_table_alias(table_alias, table_name);

        Ok(())
    }

    pub(crate) fn _bind_single_table_ref(
        &mut self,
        join_type: Option<JoinType>,
        table: &str,
        alias: Option<&TableAlias>,
    ) -> Result<(Arc<String>, LogicalPlan), DatabaseError> {
        let table_name = Arc::new(table.to_string());

        let table_catalog = self.context.table_and_bind(table_name.clone(), join_type)?;
        let scan_op = ScanOperator::build(table_name.clone(), table_catalog);

        if let Some(TableAlias { name, columns }) = alias {
            self.register_alias(columns, name.value.to_lowercase(), table_name.clone())?;
        }

        Ok((table_name, scan_op))
    }

    /// Normalize select item.
    ///
    /// - Qualified name, e.g. `SELECT t.a FROM t`
    /// - Qualified name with wildcard, e.g. `SELECT t.* FROM t,t1`
    /// - Scalar expression or aggregate expression, e.g. `SELECT COUNT(*) + 1 AS count FROM t`
    ///  
    fn normalize_select_item(
        &mut self,
        items: &[SelectItem],
    ) -> Result<Vec<ScalarExpression>, DatabaseError> {
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
                        alias: AliasType::Name(alias_name),
                    });
                }
                SelectItem::Wildcard(_) => {
                    for table_name in self.context.bind_table.keys() {
                        self.bind_table_column_refs(&mut select_items, table_name.clone())?;
                    }
                }
                SelectItem::QualifiedWildcard(table_name, _) => {
                    self.bind_table_column_refs(
                        &mut select_items,
                        Arc::new(lower_case_name(table_name)?),
                    )?;
                }
            };
        }

        Ok(select_items)
    }

    fn bind_table_column_refs(
        &self,
        exprs: &mut Vec<ScalarExpression>,
        table_name: TableName,
    ) -> Result<(), DatabaseError> {
        let table = self
            .context
            .table(table_name.clone())
            .ok_or(DatabaseError::TableNotFound)?;
        let alias_map: HashMap<&ScalarExpression, &String> = self
            .context
            .expr_aliases
            .iter()
            .filter(|(_, expr)| {
                if let ScalarExpression::ColumnRef(col) = expr {
                    return Some(&table_name) == col.table_name();
                }
                false
            })
            .map(|(alias, expr)| (expr, alias))
            .collect();
        for column in table.columns() {
            let mut expr = ScalarExpression::ColumnRef(column.clone());

            if let Some(alias_expr) = alias_map.get(&expr) {
                expr = ScalarExpression::Alias {
                    expr: Box::new(expr),
                    alias: AliasType::Name(alias_expr.to_string()),
                }
            }
            exprs.push(expr);
        }
        Ok(())
    }

    fn bind_join(
        &mut self,
        left_table: TableName,
        left: LogicalPlan,
        join: &Join,
    ) -> Result<LogicalPlan, DatabaseError> {
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
        let (right_table, right) = self.bind_single_table_ref(relation, Some(join_type))?;
        let right_table = Self::unpack_name(right_table, false);

        let left_table = self
            .context
            .table(left_table)
            .map(|table| table.schema_ref())
            .cloned()
            .ok_or(DatabaseError::TableNotFound)?;
        let right_table = self
            .context
            .table(right_table)
            .map(|table| table.schema_ref())
            .cloned()
            .ok_or(DatabaseError::TableNotFound)?;

        let on = match joint_condition {
            Some(constraint) => self.bind_join_constraint(&left_table, &right_table, constraint)?,
            None => JoinCondition::None,
        };

        Ok(LJoinOperator::build(left, right, on, join_type))
    }

    pub(crate) fn bind_where(
        &mut self,
        mut children: LogicalPlan,
        predicate: &Expr,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Where);

        let predicate = self.bind_expr(predicate)?;

        if let Some(sub_queries) = self.context.sub_queries_at_now() {
            for mut sub_query in sub_queries {
                let mut on_keys: Vec<(ScalarExpression, ScalarExpression)> = vec![];
                let mut filter = vec![];

                Self::extract_join_keys(
                    predicate.clone(),
                    &mut on_keys,
                    &mut filter,
                    children.output_schema(),
                    sub_query.output_schema(),
                )?;

                // combine multiple filter exprs into one BinaryExpr
                let join_filter = filter
                    .into_iter()
                    .reduce(|acc, expr| ScalarExpression::Binary {
                        op: BinaryOperator::And,
                        left_expr: Box::new(acc),
                        right_expr: Box::new(expr),
                        ty: LogicalType::Boolean,
                    });

                children = LJoinOperator::build(
                    children,
                    sub_query,
                    JoinCondition::On {
                        on: on_keys,
                        filter: join_filter,
                    },
                    JoinType::Inner,
                );
            }
            return Ok(children);
        }
        Ok(FilterOperator::build(predicate, children, false))
    }

    fn bind_having(
        &mut self,
        children: LogicalPlan,
        having: ScalarExpression,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Having);

        self.validate_having_orderby(&having)?;
        Ok(FilterOperator::build(having, children, true))
    }

    fn bind_project(
        &mut self,
        children: LogicalPlan,
        select_list: Vec<ScalarExpression>,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Project);

        Ok(LogicalPlan::new(
            Operator::Project(ProjectOperator { exprs: select_list }),
            vec![children],
        ))
    }

    fn bind_sort(&mut self, children: LogicalPlan, sort_fields: Vec<SortField>) -> LogicalPlan {
        self.context.step(QueryBindStep::Sort);

        LogicalPlan::new(
            Operator::Sort(SortOperator {
                sort_fields,
                limit: None,
            }),
            vec![children],
        )
    }

    fn bind_limit(
        &mut self,
        children: LogicalPlan,
        limit_expr: &Option<Expr>,
        offset_expr: &Option<Offset>,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Limit);

        let mut limit = None;
        let mut offset = None;
        if let Some(expr) = limit_expr {
            let expr = self.bind_expr(expr)?;
            match expr {
                ScalarExpression::Constant(dv) => match dv.as_ref() {
                    DataValue::Int32(Some(v)) if *v >= 0 => limit = Some(*v as usize),
                    DataValue::Int64(Some(v)) if *v >= 0 => limit = Some(*v as usize),
                    _ => return Err(DatabaseError::InvalidType),
                },
                _ => {
                    return Err(DatabaseError::InvalidColumn(
                        "invalid limit expression.".to_owned(),
                    ))
                }
            }
        }

        if let Some(expr) = offset_expr {
            let expr = self.bind_expr(&expr.value)?;
            match expr {
                ScalarExpression::Constant(dv) => match dv.as_ref() {
                    DataValue::Int32(Some(v)) if *v > 0 => offset = Some(*v as usize),
                    DataValue::Int64(Some(v)) if *v > 0 => offset = Some(*v as usize),
                    _ => return Err(DatabaseError::InvalidType),
                },
                _ => {
                    return Err(DatabaseError::InvalidColumn(
                        "invalid limit expression.".to_owned(),
                    ))
                }
            }
        }

        // TODO: validate limit and offset is correct use statistic.

        Ok(LimitOperator::build(offset, limit, children))
    }

    pub fn extract_select_join(&mut self, select_items: &mut [ScalarExpression]) {
        let bind_tables = &self.context.bind_table;
        if bind_tables.len() < 2 {
            return;
        }

        let mut table_force_nullable = Vec::with_capacity(bind_tables.len());
        let mut left_table_force_nullable = false;
        let mut left_table = None;

        for (table, join_option) in bind_tables.values() {
            if let Some(join_type) = join_option {
                let (left_force_nullable, right_force_nullable) = joins_nullable(join_type);
                table_force_nullable.push((table, right_force_nullable));
                left_table_force_nullable = left_force_nullable;
            } else {
                left_table = Some(table);
            }
        }

        if let Some(table) = left_table {
            table_force_nullable.push((table, left_table_force_nullable));
        }

        for column in select_items {
            if let ScalarExpression::ColumnRef(col) = column {
                let _ = table_force_nullable
                    .iter()
                    .find(|(table, _)| table.contains_column(col.name()))
                    .map(|(_, nullable)| {
                        let mut new_col = ColumnCatalog::clone(col);
                        new_col.nullable = *nullable;

                        *col = Arc::new(new_col);
                    });
            }
        }
    }

    fn bind_join_constraint(
        &mut self,
        left_schema: &Schema,
        right_schema: &Schema,
        constraint: &JoinConstraint,
    ) -> Result<JoinCondition, DatabaseError> {
        match constraint {
            JoinConstraint::On(expr) => {
                // left and right columns that match equi-join pattern
                let mut on_keys: Vec<(ScalarExpression, ScalarExpression)> = vec![];
                // expression that didn't match equi-join pattern
                let mut filter = vec![];
                let expr = self.bind_expr(expr)?;

                Self::extract_join_keys(
                    expr,
                    &mut on_keys,
                    &mut filter,
                    left_schema,
                    right_schema,
                )?;

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
        expr: ScalarExpression,
        accum: &mut Vec<(ScalarExpression, ScalarExpression)>,
        accum_filter: &mut Vec<ScalarExpression>,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> Result<(), DatabaseError> {
        let fn_contains = |schema: &Schema, summary: &ColumnSummary| {
            schema.iter().any(|column| summary == &column.summary)
        };
        let fn_or_contains =
            |left_schema: &Schema, right_schema: &Schema, summary: &ColumnSummary| {
                fn_contains(left_schema, summary) || fn_contains(right_schema, summary)
            };

        match expr {
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ty,
            } => {
                match op {
                    BinaryOperator::Eq => {
                        match (left_expr.as_ref(), right_expr.as_ref()) {
                            // example: foo = bar
                            (ScalarExpression::ColumnRef(l), ScalarExpression::ColumnRef(r)) => {
                                // reorder left and right joins keys to pattern: (left, right)
                                if fn_contains(left_schema, l.summary())
                                    && fn_contains(right_schema, r.summary())
                                {
                                    accum.push((*left_expr, *right_expr));
                                } else if fn_contains(left_schema, r.summary())
                                    && fn_contains(right_schema, l.summary())
                                {
                                    accum.push((*right_expr, *left_expr));
                                } else if fn_or_contains(left_schema, right_schema, l.summary())
                                    || fn_or_contains(left_schema, right_schema, r.summary())
                                {
                                    accum_filter.push(ScalarExpression::Binary {
                                        left_expr,
                                        right_expr,
                                        op,
                                        ty,
                                    });
                                }
                            }
                            (ScalarExpression::ColumnRef(column), _)
                            | (_, ScalarExpression::ColumnRef(column)) => {
                                if fn_or_contains(left_schema, right_schema, column.summary()) {
                                    accum_filter.push(ScalarExpression::Binary {
                                        left_expr,
                                        right_expr,
                                        op,
                                        ty,
                                    });
                                }
                            }
                            _other => {
                                // example: baz > 1
                                if left_expr.referenced_columns(true).iter().all(|column| {
                                    fn_or_contains(left_schema, right_schema, column.summary())
                                }) && right_expr.referenced_columns(true).iter().all(|column| {
                                    fn_or_contains(left_schema, right_schema, column.summary())
                                }) {
                                    accum_filter.push(ScalarExpression::Binary {
                                        left_expr,
                                        right_expr,
                                        op,
                                        ty,
                                    });
                                }
                            }
                        }
                    }
                    BinaryOperator::And => {
                        // example: foo = bar AND baz > 1
                        Self::extract_join_keys(
                            *left_expr,
                            accum,
                            accum_filter,
                            left_schema,
                            right_schema,
                        )?;
                        Self::extract_join_keys(
                            *right_expr,
                            accum,
                            accum_filter,
                            left_schema,
                            right_schema,
                        )?;
                    }
                    _ => {
                        if left_expr.referenced_columns(true).iter().all(|column| {
                            fn_or_contains(left_schema, right_schema, column.summary())
                        }) && right_expr.referenced_columns(true).iter().all(|column| {
                            fn_or_contains(left_schema, right_schema, column.summary())
                        }) {
                            accum_filter.push(ScalarExpression::Binary {
                                left_expr,
                                right_expr,
                                op,
                                ty,
                            });
                        }
                    }
                }
            }
            _ => {
                if expr
                    .referenced_columns(true)
                    .iter()
                    .all(|column| fn_or_contains(left_schema, right_schema, column.summary()))
                {
                    // example: baz > 1
                    accum_filter.push(expr);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::test::select_sql_run;
    use crate::errors::DatabaseError;

    #[tokio::test]
    async fn test_select_bind() -> Result<(), DatabaseError> {
        let plan_1 = select_sql_run("select * from t1").await?;
        println!("just_col:\n {:#?}", plan_1);
        let plan_2 = select_sql_run("select t1.c1, t1.c2 from t1").await?;
        println!("table_with_col:\n {:#?}", plan_2);
        let plan_3 = select_sql_run("select t1.c1, t1.c2 from t1 where c1 > 2").await?;
        println!("table_with_col_and_c1_compare_constant:\n {:#?}", plan_3);
        let plan_4 = select_sql_run("select t1.c1, t1.c2 from t1 where c1 > c2").await?;
        println!("table_with_col_and_c1_compare_c2:\n {:#?}", plan_4);
        let plan_5 = select_sql_run("select avg(t1.c1) from t1").await?;
        println!("table_with_col_and_c1_avg:\n {:#?}", plan_5);
        let plan_6 =
            select_sql_run("select t1.c1, t1.c2 from t1 where (t1.c1 - t1.c2) > 1").await?;
        println!("table_with_col_nested:\n {:#?}", plan_6);

        let plan_7 = select_sql_run("select * from t1 limit 1").await?;
        println!("limit:\n {:#?}", plan_7);

        let plan_8 = select_sql_run("select * from t1 offset 2").await?;
        println!("offset:\n {:#?}", plan_8);

        let plan_9 =
            select_sql_run("select c1, c3 from t1 inner join t2 on c1 = c3 and c1 > 1").await?;
        println!("join:\n {:#?}", plan_9);

        Ok(())
    }
}
