use crate::expression::ScalarExpression;
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::rule::column_pruning::{PushProjectIntoScan, PushProjectThroughChild};
use crate::optimizer::rule::combine_operators::{CollapseProject, CombineFilter};
use crate::optimizer::rule::pushdown_limit::{LimitProjectTranspose, EliminateLimits, PushLimitThroughJoin, PushLimitIntoTableScan};
use crate::optimizer::rule::pushdown_predicates::PushPredicateThroughJoin;

mod column_pruning;
mod combine_operators;
mod pushdown_limit;
mod pushdown_predicates;

#[derive(Debug, Copy, Clone)]
pub enum RuleImpl {
    // Column pruning
    PushProjectIntoScan,
    PushProjectThroughChild,
    // Combine operators
    CollapseProject,
    CombineFilter,
    // PushDown limit
    LimitProjectTranspose,
    EliminateLimits,
    PushLimitThroughJoin,
    PushLimitIntoTableScan,
    // PushDown predicates
    PushPredicateThroughJoin
}

impl Rule for RuleImpl {
    fn pattern(&self) -> &Pattern {
        match self {
            RuleImpl::PushProjectIntoScan => PushProjectIntoScan {}.pattern(),
            RuleImpl::PushProjectThroughChild => PushProjectThroughChild {}.pattern(),
            RuleImpl::CollapseProject => CollapseProject {}.pattern(),
            RuleImpl::CombineFilter => CombineFilter {}.pattern(),
            RuleImpl::LimitProjectTranspose => LimitProjectTranspose {}.pattern(),
            RuleImpl::EliminateLimits => EliminateLimits {}.pattern(),
            RuleImpl::PushLimitThroughJoin => PushLimitThroughJoin {}.pattern(),
            RuleImpl::PushLimitIntoTableScan => PushLimitIntoTableScan {}.pattern(),
            RuleImpl::PushPredicateThroughJoin => PushPredicateThroughJoin {}.pattern(),
        }
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) {
        match self {
            RuleImpl::PushProjectIntoScan => PushProjectIntoScan {}.apply(node_id, graph),
            RuleImpl::PushProjectThroughChild => PushProjectThroughChild {}.apply(node_id, graph),
            RuleImpl::CollapseProject => CollapseProject {}.apply(node_id, graph),
            RuleImpl::CombineFilter => CombineFilter {}.apply(node_id, graph),
            RuleImpl::LimitProjectTranspose => LimitProjectTranspose {}.apply(node_id, graph),
            RuleImpl::EliminateLimits => EliminateLimits {}.apply(node_id, graph),
            RuleImpl::PushLimitThroughJoin => PushLimitThroughJoin {}.apply(node_id, graph),
            RuleImpl::PushLimitIntoTableScan => PushLimitIntoTableScan {}.apply(node_id, graph),
            RuleImpl::PushPredicateThroughJoin => PushPredicateThroughJoin {}.apply(node_id, graph),
        }
    }
}

/// Return true when left is subset of right
pub fn is_subset_exprs(left: &[ScalarExpression], right: &[ScalarExpression]) -> bool {
    left.iter().all(|l| right.contains(l))
}