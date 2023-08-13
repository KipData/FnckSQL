use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::rule::column_pruning::{PushProjectIntoTableScan, PushProjectThroughChild};
use crate::optimizer::rule::combine_operators::{CollapseProject, CombineFilter};
use crate::optimizer::rule::pushdown_limit::{LimitProjectTranspose, EliminateLimits, PushLimitThroughJoin, PushLimitIntoTableScan};

mod column_pruning;
mod combine_operators;
mod pushdown_limit;

#[derive(Debug, Copy, Clone)]
pub enum RuleImpl {
    // Column pruning
    PushProjectIntoTableScan,
    PushProjectThroughChild,
    // Combine operators
    CollapseProject,
    CombineFilter,
    // PushDown limit
    LimitProjectTranspose,
    EliminateLimits,
    PushLimitThroughJoin,
    PushLimitIntoTableScan,
}

impl Rule for RuleImpl {
    fn pattern(&self) -> &Pattern {
        match self {
            RuleImpl::PushProjectIntoTableScan => PushProjectIntoTableScan {}.pattern(),
            RuleImpl::PushProjectThroughChild => PushProjectThroughChild {}.pattern(),
            RuleImpl::CollapseProject => CollapseProject {}.pattern(),
            RuleImpl::CombineFilter => CombineFilter {}.pattern(),
            RuleImpl::LimitProjectTranspose => LimitProjectTranspose {}.pattern(),
            RuleImpl::EliminateLimits => EliminateLimits {}.pattern(),
            RuleImpl::PushLimitThroughJoin => PushLimitThroughJoin {}.pattern(),
            RuleImpl::PushLimitIntoTableScan => PushLimitIntoTableScan {}.pattern(),
        }
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> bool {
        match self {
            RuleImpl::PushProjectIntoTableScan => PushProjectIntoTableScan {}.apply(node_id, graph),
            RuleImpl::PushProjectThroughChild => PushProjectThroughChild {}.apply(node_id, graph),
            RuleImpl::CollapseProject => CollapseProject {}.apply(node_id, graph),
            RuleImpl::CombineFilter => CombineFilter {}.apply(node_id, graph),
            RuleImpl::LimitProjectTranspose => LimitProjectTranspose {}.apply(node_id, graph),
            RuleImpl::EliminateLimits => EliminateLimits {}.apply(node_id, graph),
            RuleImpl::PushLimitThroughJoin => PushLimitThroughJoin {}.apply(node_id, graph),
            RuleImpl::PushLimitIntoTableScan => PushLimitIntoTableScan {}.apply(node_id, graph),
        }
    }
}