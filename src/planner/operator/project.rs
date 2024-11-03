use crate::expression::ScalarExpression;
use fnck_sql_serde_macros::ReferenceSerialization;
use itertools::Itertools;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct ProjectOperator {
    pub exprs: Vec<ScalarExpression>,
}

impl fmt::Display for ProjectOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let exprs = self.exprs.iter().map(|expr| format!("{}", expr)).join(", ");

        write!(f, "Projection [{}]", exprs)?;

        Ok(())
    }
}
