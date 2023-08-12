use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate, PatternMatcher};
use crate::optimizer::heuristic::batch::HepMatchOrder;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};

/// Use pattern to determines which rule can be applied
pub struct HepMatcher<'a, 'b> {
    pattern: &'a Pattern,
    start_id: HepNodeId,
    graph: &'b HepGraph,
}

impl<'a, 'b> HepMatcher<'a, 'b> {
    pub(crate) fn new(pattern: &'a Pattern, start_id: HepNodeId, graph: &'b HepGraph) -> Self {
        Self {
            pattern,
            start_id,
            graph,
        }
    }
}

impl PatternMatcher for HepMatcher<'_, '_> {
    fn match_opt_expr(&self) -> bool {
        let op = self.graph.operator(self.start_id);
        // check the root node predicate
        if !(self.pattern.predicate)(&op) {
            return false;
        }

        match &self.pattern.children {
            PatternChildrenPredicate::MatchedRecursive => {
                // check
                for node_id in self.graph.nodes_iter(HepMatchOrder::TopDown, Some(self.start_id)) {
                    if !(self.pattern.predicate)(&self.graph.operator(node_id)) {
                        return false;
                    }
                }
            }
            PatternChildrenPredicate::Predicate(patterns) => {
                for node_id in self.graph.children_at(self.start_id) {
                    for pattern in patterns {
                        if !HepMatcher::new(pattern, node_id, self.graph).match_opt_expr() {
                            return false;
                        }
                    }
                }
            }
            PatternChildrenPredicate::None => ()
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use crate::binder::test::select_sql_run;
    use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate, PatternMatcher};
    use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
    use crate::optimizer::heuristic::matcher::HepMatcher;
    use crate::planner::LogicalPlan;
    use crate::planner::operator::Operator;

    #[test]
    fn test_predicate() -> Result<()> {
        let plan = select_sql_run("select * from t1")?;
        let graph = HepGraph::new(plan.clone());

        let project_into_table_scan_pattern = Pattern {
            predicate: |p| match p {
                Operator::Project(_) => true,
                _ => false,
            },
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| match p {
                    Operator::Scan(_) => true,
                    _ => false,
                },
                children: PatternChildrenPredicate::None,
            }]),
        };

        assert!(
            HepMatcher::new(&project_into_table_scan_pattern, HepNodeId::new(0), &graph)
                .match_opt_expr()
        );

        Ok(())
    }

    #[test]
    fn test_recursive() -> Result<()> {
        let all_dummy_plan = LogicalPlan {
            operator: Operator::Dummy,
            childrens: vec![
                LogicalPlan {
                    operator: Operator::Dummy,
                    childrens: vec![
                        LogicalPlan {
                            operator: Operator::Dummy,
                            childrens: vec![],
                        }
                    ],
                },
                LogicalPlan {
                    operator: Operator::Dummy,
                    childrens: vec![],
                }
            ],
        };
        let graph = HepGraph::new(all_dummy_plan.clone());

        let only_dummy_pattern = Pattern {
            predicate: |p| match p {
                Operator::Dummy => true,
                _ => false,
            },
            children: PatternChildrenPredicate::MatchedRecursive,
        };

        assert!(
            HepMatcher::new(&only_dummy_pattern, HepNodeId::new(0), &graph)
                .match_opt_expr()
        );

        Ok(())
    }
}