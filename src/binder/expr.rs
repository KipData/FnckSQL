use crate::binder::BindError;
use anyhow::Result;
use itertools::Itertools;
use sqlparser::ast::{BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, Ident};
use std::slice;
use crate::expression::agg::AggKind;

use super::Binder;
use crate::expression::ScalarExpression;
use crate::types::LogicalType;

impl Binder {
    pub(crate) fn bind_expr(&mut self, expr: &Expr) -> Result<ScalarExpression> {
        match expr {
            Expr::Identifier(ident) => {
                self.bind_column_ref_from_identifiers(slice::from_ref(ident))
            }
            Expr::CompoundIdentifier(idents) => {
                self.bind_column_ref_from_identifiers(idents)
            }
            Expr::BinaryOp { left, right, op} => {
                self.bind_binary_op_internal(left, right, op)
            }
            Expr::Value(v) => Ok(ScalarExpression::Constant(v.into())),
            Expr::Function(func) => self.bind_agg_call(func),
            Expr::Nested(expr) => self.bind_expr(expr),
            _ => {
                todo!()
            }
        }
    }

    pub fn bind_column_ref_from_identifiers(
        &mut self,
        idents: &[Ident],
    ) -> Result<ScalarExpression> {
        let idents = idents
            .iter()
            .map(|ident| Ident::new(ident.value.to_lowercase()))
            .collect_vec();
        let (_schema_name, table_name, column_name) = match idents.as_slice() {
            [column] => (None, None, &column.value),
            [table, column] => (None, Some(&table.value), &column.value),
            [schema, table, column] => (Some(&schema.value), Some(&table.value), &column.value),
            _ => {
                return Err(BindError::InvalidColumn(
                    idents
                        .iter()
                        .map(|ident| ident.value.clone())
                        .join(".")
                        .to_string(),
                )
                .into())
            }
        };

        if let Some(table) = table_name {
            let table_catalog = self
                .context
                .catalog
                .get_table_by_name(table)
                .ok_or_else(|| BindError::InvalidTable(table.to_string()))?;

            let column_catalog = table_catalog
                .get_column_by_name(column_name)
                .ok_or_else(|| BindError::InvalidColumn(column_name.to_string()))?;
            Ok(ScalarExpression::ColumnRef(column_catalog.clone()))
        } else {
            // handle col syntax
            let mut got_column = None;
            for (_, table_catalog) in self.context.catalog.tables() {
                if let Some(column_catalog) = table_catalog.get_column_by_name(column_name) {
                    if got_column.is_some() {
                        return Err(BindError::InvalidColumn(column_name.to_string()).into());
                    }
                    got_column = Some(column_catalog);
                }
            }
            if got_column.is_none() {
                if let Some(expr) = self.context.aliases.get(column_name) {
                    return Ok(expr.clone());
                }
            }
            let column_catalog =
                got_column.ok_or_else(|| BindError::InvalidColumn(column_name.to_string()))?;
            Ok(ScalarExpression::ColumnRef(column_catalog.clone()))
        }
    }

    fn bind_binary_op_internal(
        &mut self,
        left: &Expr,
        right: &Expr,
        op: &BinaryOperator,
    ) -> Result<ScalarExpression> {
        let left_expr = Box::new(self.bind_expr(left)?);
        let right_expr = Box::new(self.bind_expr(right)?);

        let ty = match op {
            BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Multiply |
            BinaryOperator::Divide | BinaryOperator::Modulo => {
                LogicalType::max_logical_type(
                    &left_expr.return_type(),
                    &right_expr.return_type()
                )?
            }
            BinaryOperator::Gt | BinaryOperator::Lt | BinaryOperator::GtEq |
            BinaryOperator::LtEq | BinaryOperator::Eq | BinaryOperator::NotEq |
            BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Xor => {
                LogicalType::Boolean
            },
            _ => todo!()
        };

        Ok(ScalarExpression::Binary {
            op: (op.clone()).into(),
            left_expr,
            right_expr,
            ty,
        })
    }

    fn bind_agg_call(&mut self, func: &Function) -> Result<ScalarExpression> {
        let args: Vec<ScalarExpression> = func.args
            .iter()
            .map(|arg| {
                let arg_expr = match arg {
                    FunctionArg::Named { arg, .. } => arg,
                    FunctionArg::Unnamed(arg) => arg,
                };
                match arg_expr {
                    FunctionArgExpr::Expr(expr) => self.bind_expr(expr),
                    _ => todo!()
                }
            })
            .try_collect()?;
        let ty: LogicalType = args[0].return_type();

        Ok(match func.name.to_string().to_lowercase().as_str() {
            "count" => ScalarExpression::AggCall{
                kind: AggKind::Count,
                args,
                ty: LogicalType::UInteger,
            },
            "sum" => ScalarExpression::AggCall{
                kind: AggKind::Sum,
                args,
                ty,
            },
            "min" => ScalarExpression::AggCall{
                kind: AggKind::Min,
                args,
                ty,
            },
            "max" => ScalarExpression::AggCall{
                kind: AggKind::Max,
                args,
                ty,
            },
            "avg" => ScalarExpression::AggCall{
                kind: AggKind::Avg,
                args,
                ty,
            },
            _ => todo!(),
        })
    }
}
