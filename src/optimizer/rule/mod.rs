use crate::expression::ScalarExpression;
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::rule::column_pruning::ColumnPruning;
use crate::optimizer::rule::combine_operators::{CollapseProject, CombineFilter};
use crate::optimizer::rule::pushdown_limit::{
    EliminateLimits, LimitProjectTranspose, PushLimitIntoScan, PushLimitThroughJoin,
};
use crate::optimizer::rule::pushdown_predicates::PushPredicateIntoScan;
use crate::optimizer::rule::pushdown_predicates::PushPredicateThroughJoin;
use crate::optimizer::rule::simplification::SimplifyFilter;
use crate::optimizer::rule::simplification::{ConstantCalculation, LikeRewrite};
use crate::optimizer::OptimizerError;

mod column_pruning;
mod combine_operators;
mod pushdown_limit;
mod pushdown_predicates;
mod simplification;

#[derive(Debug, Copy, Clone)]
pub enum RuleImpl {
    ColumnPruning,
    // Combine operators
    CollapseProject,
    CombineFilter,
    // PushDown limit
    LimitProjectTranspose,
    EliminateLimits,
    PushLimitThroughJoin,
    PushLimitIntoTableScan,
    // PushDown predicates
    PushPredicateThroughJoin,
    // Tips: need to be used with `SimplifyFilter`
    PushPredicateIntoScan,
    // Simplification
    SimplifyFilter,
    ConstantCalculation,
    LikeRewrite,
}

impl Rule for RuleImpl {
    fn pattern(&self) -> &Pattern {
        match self {
            RuleImpl::ColumnPruning => ColumnPruning.pattern(),
            RuleImpl::CollapseProject => CollapseProject.pattern(),
            RuleImpl::CombineFilter => CombineFilter.pattern(),
            RuleImpl::LimitProjectTranspose => LimitProjectTranspose.pattern(),
            RuleImpl::EliminateLimits => EliminateLimits.pattern(),
            RuleImpl::PushLimitThroughJoin => PushLimitThroughJoin.pattern(),
            RuleImpl::PushLimitIntoTableScan => PushLimitIntoScan.pattern(),
            RuleImpl::PushPredicateThroughJoin => PushPredicateThroughJoin.pattern(),
            RuleImpl::PushPredicateIntoScan => PushPredicateIntoScan.pattern(),
            RuleImpl::SimplifyFilter => SimplifyFilter.pattern(),
            RuleImpl::ConstantCalculation => ConstantCalculation.pattern(),
            RuleImpl::LikeRewrite => LikeRewrite.pattern(),
        }
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        match self {
            RuleImpl::ColumnPruning => ColumnPruning.apply(node_id, graph),
            RuleImpl::CollapseProject => CollapseProject.apply(node_id, graph),
            RuleImpl::CombineFilter => CombineFilter.apply(node_id, graph),
            RuleImpl::LimitProjectTranspose => LimitProjectTranspose.apply(node_id, graph),
            RuleImpl::EliminateLimits => EliminateLimits.apply(node_id, graph),
            RuleImpl::PushLimitThroughJoin => PushLimitThroughJoin.apply(node_id, graph),
            RuleImpl::PushLimitIntoTableScan => PushLimitIntoScan.apply(node_id, graph),
            RuleImpl::PushPredicateThroughJoin => PushPredicateThroughJoin.apply(node_id, graph),
            RuleImpl::SimplifyFilter => SimplifyFilter.apply(node_id, graph),
            RuleImpl::PushPredicateIntoScan => PushPredicateIntoScan.apply(node_id, graph),
            RuleImpl::ConstantCalculation => ConstantCalculation.apply(node_id, graph),
            RuleImpl::LikeRewrite => LikeRewrite.apply(node_id, graph),
        }
    }
}

/// Return true when left is subset of right
pub fn is_subset_exprs(left: &[ScalarExpression], right: &[ScalarExpression]) -> bool {
    left.iter().all(|l| right.contains(l))
}
