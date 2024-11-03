use crate::binder::copy::ExtSource;
use fnck_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct CopyToFileOperator {
    pub source: ExtSource,
}
