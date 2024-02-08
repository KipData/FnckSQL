use crate::errors::DatabaseError;
use crate::optimizer::core::column_meta::ColumnMetaLoader;
use crate::optimizer::core::memo::Memo;
use crate::optimizer::core::pattern::PatternMatcher;
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::heuristic::batch::{HepBatch, HepBatchStrategy};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::heuristic::matcher::HepMatcher;
use crate::optimizer::rule::implementation::ImplementationRuleImpl;
use crate::optimizer::rule::normalization::NormalizationRuleImpl;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use std::ops::Not;

pub struct HepOptimizer {
    batches: Vec<HepBatch>,
    pub graph: HepGraph,
    implementations: Vec<ImplementationRuleImpl>,
}

impl HepOptimizer {
    pub fn new(root: LogicalPlan) -> Self {
        Self {
            batches: vec![],
            graph: HepGraph::new(root),
            implementations: vec![],
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

    pub fn implementations(mut self, implementations: Vec<ImplementationRuleImpl>) -> Self {
        self.implementations = implementations;
        self
    }

    pub fn find_best<T: Transaction>(
        mut self,
        loader: Option<&ColumnMetaLoader<'_, T>>,
    ) -> Result<LogicalPlan, DatabaseError> {
        for ref batch in self.batches {
            let mut batch_over = false;
            let mut iteration = 1usize;

            while iteration <= batch.strategy.max_iteration && !batch_over {
                if Self::apply_batch(&mut self.graph, batch)? {
                    iteration += 1;
                } else {
                    batch_over = true
                }
            }
        }
        let memo = loader
            .and_then(|loader| {
                self.implementations
                    .is_empty()
                    .not()
                    .then(|| Memo::new(&self.graph, loader, &self.implementations))
            })
            .transpose()?;

        self
            .graph
            .into_plan(memo.as_ref())
            .ok_or(DatabaseError::EmptyPlan)
    }

    fn apply_batch(
        graph: &mut HepGraph,
        HepBatch {
            rules, strategy, ..
        }: &HepBatch,
    ) -> Result<bool, DatabaseError> {
        let before_version = graph.version;

        for rule in rules {
            for node_id in graph.nodes_iter(strategy.match_order, None) {
                if Self::apply_rule(graph, rule, node_id)? {
                    break;
                }
            }
        }

        Ok(before_version != graph.version)
    }

    fn apply_rule(
        graph: &mut HepGraph,
        rule: &NormalizationRuleImpl,
        node_id: HepNodeId,
    ) -> Result<bool, DatabaseError> {
        let before_version = graph.version;

        if HepMatcher::new(rule.pattern(), node_id, graph).match_opt_expr() {
            rule.apply(node_id, graph)?;
        }

        Ok(before_version != graph.version)
    }
}
