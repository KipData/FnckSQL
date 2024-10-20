use crate::planner::LogicalPlan;
use serde_macros::ReferenceSerialization;

#[derive(Debug, Clone, Hash, Eq, PartialEq, ReferenceSerialization)]
pub struct View {
    pub name: String,
    pub plan: LogicalPlan,
}
