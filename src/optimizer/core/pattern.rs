use crate::optimizer::core::opt_expr::OptExpr;
use crate::planner::operator::Operator;

#[allow(dead_code)]
pub enum PatternChildrenPredicate {
    /// all childrens nodes match all
    MatchedRecursive,
    /// childrens nodes are matched according to paterns order
    Predicate(Vec<Pattern>),
    /// childrens jump out match
    None,
}

/// The pattern tree to match a plan tree. It defined in `Rule` and used in `PatternMatcher`.
pub struct Pattern {
    /// The root node predicate, not contains the children.
    pub predicate: fn(&Operator) -> bool,
    /// The children's predicate of current node.
    pub children: PatternChildrenPredicate,
}

pub trait PatternMatcher {
    fn match_opt_expr(&self) -> bool;
}