use crate::optimizer::core::opt_expr::OptExpr;
use crate::planner::LogicalPlan;

#[allow(dead_code)]
pub enum PatternChildrenPredicate {
    /// All children and their children are matched and will be collected as
    /// `OptExprNode::PlanRef`. Currently used in one-time-applied rule.
    MatchedRecursive,
    /// All children will be evaluated in `PatternMatcher`, if pattern match, node will collected
    /// as `OptExprNode::PlanRef`. if vec is empty, it means no children are matched and
    /// collected.
    Predicate(Vec<Pattern>),
    /// We don't care the children, and them will be collected as existing nodes
    /// `OptExprNode::OptExpr` in OptExpr tree.
    None,
}

/// The pattern tree to match a plan tree. It defined in `Rule` and used in `PatternMatcher`.
pub struct Pattern {
    /// The root node predicate, not contains the children.
    pub predicate: fn(&LogicalPlan) -> bool,
    /// The children's predicate of current node.
    pub children: PatternChildrenPredicate,
}

/// The matcher use a pattern tree to match a plan tree.
///
/// Result organized in `OptExpr`. Matched nodes are `OptExprNode::PlanRef`, and non-matched
/// children nodes are `OptExprNode::OptExpr`.
pub trait PatternMatcher {
    fn match_opt_expr(&self) -> Option<OptExpr>;
}