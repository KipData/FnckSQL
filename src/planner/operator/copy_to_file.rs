use crate::binder::copy::ExtSource;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct CopyToFileOperator {
    pub source: ExtSource,
}
