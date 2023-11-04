use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use std::fmt::Debug;

pub type OptExprNodeId = usize;
#[derive(Clone, Debug)]
pub struct OptExpr {
    /// The root of the tree.
    pub root: Operator,
    /// The root's children expressions.
    pub childrens: Vec<OptExpr>,
}

impl OptExpr {
    #[allow(dead_code)]
    pub fn new(root: Operator, childrens: Vec<OptExpr>) -> Self {
        Self { root, childrens }
    }

    #[allow(dead_code)]
    pub fn new_from_op_ref(plan: &LogicalPlan) -> Self {
        OptExpr::build_opt_expr_internal(plan)
    }

    #[allow(dead_code)]
    fn build_opt_expr_internal(input: &LogicalPlan) -> OptExpr {
        let root = input.operator.clone();
        let childrens = input
            .childrens
            .iter()
            .map(OptExpr::build_opt_expr_internal)
            .collect::<Vec<_>>();
        OptExpr { root, childrens }
    }

    #[allow(dead_code)]
    pub fn to_plan_ref(&self) -> LogicalPlan {
        let childrens = self
            .childrens
            .iter()
            .map(|c| c.to_plan_ref())
            .collect::<Vec<_>>();
        LogicalPlan {
            operator: self.root.clone(),
            childrens,
        }
    }
}
