use crate::optimizer::rule::normalization::NormalizationRuleImpl;

/// A batch of rules.
#[derive(Clone)]
pub struct HepBatch {
    pub name: String,
    pub strategy: HepBatchStrategy,
    pub rules: Vec<NormalizationRuleImpl>,
}

impl HepBatch {
    pub fn new(
        name: String,
        strategy: HepBatchStrategy,
        rules: Vec<NormalizationRuleImpl>,
    ) -> Self {
        Self {
            name,
            strategy,
            rules,
        }
    }
}

#[derive(Clone)]
pub struct HepBatchStrategy {
    /// An execution_ap strategy for rules that indicates the maximum number of executions. If the
    /// execution_ap reaches fix point (i.e. converge) before maxIterations, it will stop.
    ///
    /// Fix Point means that plan tree not changed after applying all rules.
    pub max_iteration: usize,
    /// An order to traverse the plan tree nodes.
    pub match_order: HepMatchOrder,
}

impl HepBatchStrategy {
    pub fn once_topdown() -> Self {
        HepBatchStrategy {
            max_iteration: 1,
            match_order: HepMatchOrder::TopDown,
        }
    }

    pub fn fix_point_topdown(max_iteration: usize) -> Self {
        HepBatchStrategy {
            max_iteration,
            match_order: HepMatchOrder::TopDown,
        }
    }
}

#[derive(Clone, Copy)]
pub enum HepMatchOrder {
    /// Match from root down. A match attempt at an ancestor always precedes all match attempts at
    /// its descendants.
    TopDown,
    /// Match from leaves up. A match attempt at a descendant precedes all match attempts at its
    /// ancestors.
    #[allow(dead_code)]
    BottomUp,
}
