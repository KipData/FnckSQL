use crate::optimizer::core::opt_expr::OptExpr;
use crate::optimizer::core::pattern::Pattern;

/// A rule is to transform logically equivalent expression. There are two kinds of rules:
///
/// - Transformation Rule: Logical to Logical
/// - Implementation Rule: Logical to Physical
pub trait Rule {
    /// The pattern to determine whether the rule can be applied.
    fn pattern(&self) -> &Pattern;

    /// Apply the rule and write the transformation result to `Substitute`.
    /// The pattern tree determines the opt_expr tree internal nodes type.
    fn apply(&self, opt_expr: OptExpr) -> OptExpr;
}