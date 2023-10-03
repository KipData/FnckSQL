use crate::binder::copy::ExtSource;

#[derive(Debug, PartialEq, Clone)]
pub struct CopyToFileOperator {
    pub source: ExtSource,
}