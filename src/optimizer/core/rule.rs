use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};

/// A rule is to transform logically equivalent expression
pub trait Rule {
    /// The pattern to determine whether the rule can be applied.
    fn pattern(&self) -> &Pattern;

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph);
}