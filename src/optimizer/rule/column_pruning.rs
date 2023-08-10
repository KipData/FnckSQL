use lazy_static::lazy_static;
use crate::optimizer::core::opt_expr::OptExprNode;
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::planner::operator::Operator;

lazy_static! {
    static ref PUSH_PROJECT_INTO_TABLE_SCAN_RULE: Pattern = {
        Pattern {
            predicate: |op| match op {
                Operator::Project(_) => true,
                _ => false
            },
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| match op {
                    Operator::Scan(_) => true,
                    _ => false
                },
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

#[derive(Copy, Clone)]
pub struct PushProjectIntoTableScan;

impl Rule for PushProjectIntoTableScan {
    fn pattern(&self) -> &Pattern {
        &PUSH_PROJECT_INTO_TABLE_SCAN_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> bool {
        if let Operator::Project(project_op) = graph.operator(node_id) {
            let child_index = graph.children_at(node_id)[0];
            if let Operator::Scan(scan_op) = graph.operator(child_index) {
                let mut new_scan_op = scan_op.clone();

                new_scan_op.columns.append(&mut project_op.columns.clone());
                new_scan_op.columns.dedup();

                graph.remove_node(node_id, false);
                graph.replace_node(
                    child_index,
                    OptExprNode::OperatorRef(Operator::Scan(new_scan_op))
                );
                return true;
            }
        }

        false
    }
}