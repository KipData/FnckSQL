use std::fmt::Debug;
use crate::planner::LogicalPlan;
use crate::planner::operator::Operator;

pub type OptExprNodeId = usize;

#[derive(Clone, PartialEq)]
pub enum OptExprNode {
    /// Raw plan node with dummy children.
    OperatorRef(Operator),
    /// Existing OptExprNode in graph.
    OptExpr(OptExprNodeId),
}

impl Debug for OptExprNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OperatorRef(op) => write!(f, "LogicOperator({:?})", op),
            Self::OptExpr(id) => write!(f, "OptExpr({})", id),
        }
    }
}

impl OptExprNode {
    pub fn get_operator(&self) -> &Operator {
        match self {
            OptExprNode::OperatorRef(op) => op,
            OptExprNode::OptExpr(_) => {
                panic!("OptExprNode::get_plan_ref() called on OptExprNode::OptExpr")
            }
        }
    }
}

/// A sub-plan-tree representation used in Rule and Matcher. Every root node could be new node or
/// existing graph node. For new node, it will be added in graph, for existing node, it will be
/// reconnect in graph later.
///
/// It constructed by `PatternMatcher` when optimizer to match a rule, and consumed by `Rule` to do
/// transformation, and `Rule` return new `OptExpr` to replace the matched sub-tree.
#[derive(Clone, Debug)]
pub struct OptExpr {
    /// The root of the tree.
    pub root: OptExprNode,
    /// The root's children expressions.
    pub children: Vec<OptExpr>,
}


impl OptExpr {
    pub fn new(root: OptExprNode, children: Vec<OptExpr>) -> Self {
        Self { root, children }
    }

    /// Create OptExpr tree from OperatorRef tree, it will change all nodes' children to dummy nodes.
    pub fn new_from_op_ref(plan: &LogicalPlan) -> Self {
        OptExpr::build_opt_expr_internal(plan)
    }

    fn build_opt_expr_internal(input: &LogicalPlan) -> OptExpr {
        // FIXME: clone with dummy children to fix comments in PatternMatcher.
        let root = OptExprNode::OperatorRef(input.operator.clone());
        let children = input
            .childrens
            .iter()
            .map(OptExpr::build_opt_expr_internal)
            .collect::<Vec<_>>();
        OptExpr { root, children }
    }

    pub fn to_plan_ref(&self) -> LogicalPlan {
        match &self.root {
            OptExprNode::OperatorRef(op) => {
                let childrens = self
                    .children
                    .iter()
                    .map(|c| c.to_plan_ref())
                    .collect::<Vec<_>>();
                LogicalPlan {
                    operator: op.clone(),
                    childrens,
                }
            }
            OptExprNode::OptExpr(_) => LogicalPlan {
                operator: Operator::Dummy,
                childrens: vec![],
            },
        }
    }
}
