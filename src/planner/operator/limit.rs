use crate::planner::LogicalPlan;
use std::fmt;
use std::fmt::Formatter;

use super::Operator;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct LimitOperator {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}

impl LimitOperator {
    pub fn build(
        offset: Option<usize>,
        limit: Option<usize>,
        children: LogicalPlan,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Limit(LimitOperator { offset, limit }),
            vec![children],
        )
    }
}

impl fmt::Display for LimitOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if let Some(limit) = self.limit {
            write!(f, "Limit {}", limit)?;
        }
        if self.limit.is_some() && self.offset.is_some() {
            write!(f, ", ")?;
        }
        if let Some(offset) = self.offset {
            write!(f, "Offset {}", offset)?;
        }

        Ok(())
    }
}
