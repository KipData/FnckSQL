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

#[derive(Clone, Debug)]
pub struct OptExpr {
    /// The root of the tree.
    pub root: OptExprNode,
    /// The root's children expressions.
    pub childrens: Vec<OptExpr>,
}


impl OptExpr {
    pub fn new(root: OptExprNode, childrens: Vec<OptExpr>) -> Self {
        Self { root, childrens }
    }

    pub fn new_from_op_ref(plan: &LogicalPlan) -> Self {
        OptExpr::build_opt_expr_internal(plan)
    }

    fn build_opt_expr_internal(input: &LogicalPlan) -> OptExpr {
        let root = OptExprNode::OperatorRef(input.operator.clone());
        let childrens = input
            .childrens
            .iter()
            .map(OptExpr::build_opt_expr_internal)
            .collect::<Vec<_>>();
        OptExpr { root, childrens }
    }

    pub fn to_plan_ref(&self) -> LogicalPlan {
        match &self.root {
            OptExprNode::OperatorRef(op) => {
                let childrens = self
                    .childrens
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
