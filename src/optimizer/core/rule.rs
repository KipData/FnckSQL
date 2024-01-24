use crate::optimizer::core::memo::GroupExpression;
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::OptimizerError;
use crate::planner::operator::Operator;

// TODO: Use indexing and other methods for matching optimization to avoid traversal
pub trait MatchPattern {
    fn pattern(&self) -> &Pattern;
}

pub trait NormalizationRule: MatchPattern {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError>;
}

pub trait ImplementationRule: MatchPattern {
    fn to_expression(
        &self,
        op: &Operator,
        group_expr: &mut GroupExpression,
    ) -> Result<(), OptimizerError>;
}
