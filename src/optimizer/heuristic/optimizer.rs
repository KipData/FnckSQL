use itertools::Itertools;
use crate::optimizer::core::pattern::PatternMatcher;
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::batch::{HepBatch, HepBatchStrategy};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::heuristic::matcher::HepMatcher;
use crate::optimizer::rule::RuleImpl;
use crate::planner::LogicalPlan;

pub struct HepOptimizer {
    batches: Vec<HepBatch>,
    pub graph: HepGraph,
}

impl HepOptimizer {
    pub fn new(root: LogicalPlan) -> Self {
        Self {
            batches: vec![],
            graph: HepGraph::new(root),
        }
    }

    pub fn batch(mut self, name: String, strategy: HepBatchStrategy, rules: Vec<RuleImpl>) -> Self {
        self.batches.push(HepBatch::new(name, strategy, rules));
        self
    }

    pub fn find_best(&mut self) -> LogicalPlan {
        let batches = self.batches.clone();

        for batch in batches {
            let mut batch_over = false;
            let mut iteration = 1usize;

            while iteration <= batch.strategy.max_iteration && !batch_over {
                if self.apply_batch(&batch) {
                    iteration += 1;
                } else {
                    batch_over = true
                }
            }
        }

        self.graph.to_plan()
    }

    fn apply_batch(&mut self, HepBatch{ rules, strategy, .. }: &HepBatch) -> bool {
        let mut has_apply = false;

        for rule in rules {
            for node_id in self.graph.nodes_iter(strategy.match_order, None) {
                if self.apply_rule(rule, node_id) {
                    has_apply = true;
                    break;
                }
            }
        }

        has_apply
    }

    fn apply_rule(&mut self, rule: &RuleImpl, node_id: HepNodeId) -> bool {
        HepMatcher::new(rule.pattern(), node_id, &self.graph)
            .match_opt_expr()
            .then(|| rule.apply(node_id, &mut self.graph))
            .unwrap_or(false)
    }

}