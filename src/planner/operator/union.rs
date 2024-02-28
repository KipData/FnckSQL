use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::types::tuple::SchemaRef;
use itertools::Itertools;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct UnionOperator {
    pub schema_ref: SchemaRef,
}

impl UnionOperator {
    pub fn build(
        schema_ref: SchemaRef,
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Union(UnionOperator { schema_ref }),
            vec![left_plan, right_plan],
        )
    }
}

impl fmt::Display for UnionOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let schema = self
            .schema_ref
            .iter()
            .map(|column| column.name().to_string())
            .join(", ");

        write!(f, "Union: [{}]", schema)?;

        Ok(())
    }
}
