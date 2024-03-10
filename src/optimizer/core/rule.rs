use crate::errors::DatabaseError;
use crate::optimizer::core::memo::GroupExpression;
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::planner::operator::Operator;
use crate::storage::Transaction;

// TODO: Use indexing and other methods for matching optimization to avoid traversal
pub trait MatchPattern {
    fn pattern(&self) -> &Pattern;
}

pub trait NormalizationRule: MatchPattern {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError>;
}

pub trait ImplementationRule<T: Transaction>: MatchPattern {
    fn to_expression(
        &self,
        op: &Operator,
        loader: &StatisticMetaLoader<T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), DatabaseError>;
}
