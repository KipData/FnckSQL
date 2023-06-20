use sqlparser::ast::Expr;

use super::Binder;
use crate::expression::ScalarExpression;
use anyhow::Result;

impl Binder {
    pub(crate) fn bind_expr(&mut self, expr: &Expr) -> Result<ScalarExpression> {
        todo!()
    }
}
