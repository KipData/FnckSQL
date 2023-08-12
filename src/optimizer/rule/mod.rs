use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::rule::column_pruning::{PushProjectIntoTableScan, PushProjectThroughChild};

mod column_pruning;

#[derive(Debug, Copy, Clone)]
pub enum RuleImpl {
    PushProjectIntoTableScan,
    PushProjectThroughChild,
}

impl Rule for RuleImpl {
    fn pattern(&self) -> &Pattern {
        match self {
            RuleImpl::PushProjectIntoTableScan => PushProjectIntoTableScan {}.pattern(),
            RuleImpl::PushProjectThroughChild => PushProjectThroughChild {}.pattern(),
        }
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> bool {
        match self {
            RuleImpl::PushProjectIntoTableScan => PushProjectIntoTableScan {}.apply(node_id, graph),
            RuleImpl::PushProjectThroughChild => PushProjectThroughChild {}.apply(node_id, graph),
        }
    }
}