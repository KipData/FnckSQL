use crate::optimizer::core::pattern::PatternMatcher;
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::batch::HepBatch;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::heuristic::matcher::HepMatcher;
use crate::optimizer::rule::RuleImpl;
use crate::planner::LogicalPlan;

pub struct HepOptimizer {
    batches: Vec<HepBatch>,
    graph: HepGraph,
}

impl HepOptimizer {
    pub fn new(root: LogicalPlan) -> Self {
        Self {
            batches: vec![],
            graph: HepGraph::new(root),
        }
    }

    pub fn batch(mut self, batch: HepBatch) -> Self {
        self.batches.push(batch);
        self
    }

    pub fn find_best(&mut self) -> LogicalPlan {
        let batches = self.batches.clone();

        for batch in batches {
            let mut iteration = 1usize;

            while iteration <= batch.strategy.max_iteration {
                if self.apply_batch(&batch) {
                    break
                }

                iteration += 1;
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

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use crate::binder::test::select_sql_run;
    use crate::optimizer::heuristic::batch::{HepBatch, HepBatchStrategy};
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::RuleImpl;
    use crate::planner::operator::Operator;

    #[test]
    fn test_project_into_table_scan() -> Result<()> {
        let plan = select_sql_run("select * from t1")?;

        let best_plan = HepOptimizer::new(plan.clone())
            .batch(HepBatch::new(
                "test_project_into_table_scan".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![RuleImpl::PushProjectIntoTableScan]
            ))
            .find_best();

        assert_eq!(best_plan.childrens.len(), 0);
        match best_plan.operator {
            Operator::Scan(op) => {
                assert_eq!(op.columns.len(), 2);
            },
            _ => unreachable!("Should be a scan operator"),
        }

        Ok(())
    }
}