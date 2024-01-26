use crate::optimizer::core::histogram::HistogramLoader;
use crate::optimizer::core::memo::Memo;
use crate::optimizer::core::pattern::PatternMatcher;
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::heuristic::batch::{HepBatch, HepBatchStrategy};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::heuristic::matcher::HepMatcher;
use crate::optimizer::rule::implementation::ImplementationRuleImpl;
use crate::optimizer::rule::normalization::NormalizationRuleImpl;
use crate::optimizer::OptimizerError;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;

pub struct HepOptimizer {
    batches: Vec<HepBatch>,
    memo: Option<Memo>,
    pub graph: HepGraph,
}

impl HepOptimizer {
    pub fn new(root: LogicalPlan) -> Self {
        Self {
            batches: vec![],
            memo: None,
            graph: HepGraph::new(root),
        }
    }

    pub fn batch(
        mut self,
        name: String,
        strategy: HepBatchStrategy,
        rules: Vec<NormalizationRuleImpl>,
    ) -> Self {
        self.batches.push(HepBatch::new(name, strategy, rules));
        self
    }

    pub fn build_memo<T: Transaction>(
        mut self,
        loader: &HistogramLoader<'_, T>,
        implementations: &[ImplementationRuleImpl],
    ) -> Result<Self, OptimizerError> {
        self.memo = Some(Memo::new(&self.graph, &loader, implementations)?);

        Ok(self)
    }

    pub fn find_best(mut self) -> Result<LogicalPlan, OptimizerError> {
        let batches = self.batches.clone();

        for batch in batches {
            let mut batch_over = false;
            let mut iteration = 1usize;

            while iteration <= batch.strategy.max_iteration && !batch_over {
                if self.apply_batch(&batch)? {
                    iteration += 1;
                } else {
                    batch_over = true
                }
            }
        }

        Ok(self.graph.to_plan(None).ok_or(OptimizerError::EmptyPlan)?)
    }

    fn apply_batch(
        &mut self,
        HepBatch {
            rules, strategy, ..
        }: &HepBatch,
    ) -> Result<bool, OptimizerError> {
        let start_ver = self.graph.version;

        for rule in rules {
            for node_id in self.graph.nodes_iter(strategy.match_order, None) {
                if self.apply_rule(rule, node_id)? {
                    break;
                }
            }
        }

        Ok(start_ver != self.graph.version)
    }

    fn apply_rule(
        &mut self,
        rule: &NormalizationRuleImpl,
        node_id: HepNodeId,
    ) -> Result<bool, OptimizerError> {
        let after_version = self.graph.version;

        if HepMatcher::new(rule.pattern(), node_id, &self.graph).match_opt_expr() {
            rule.apply(node_id, &mut self.graph)?;
        }

        Ok(after_version != self.graph.version)
    }
}
