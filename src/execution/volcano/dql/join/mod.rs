use crate::planner::operator::join::JoinType;

pub(crate) mod hash_join;

pub fn joins_nullable(join_type: &JoinType) -> (bool, bool) {
    match join_type {
        JoinType::Inner => (false, false),
        JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti => (false, true),
        JoinType::RightOuter => (true, false),
        JoinType::Full => (true, true),
        JoinType::Cross => (true, true),
    }
}
