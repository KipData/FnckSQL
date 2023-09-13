#[derive(thiserror::Error, Debug)]
pub enum TypeError {
    #[error("invalid logical type")]
    InvalidLogicalType,
    #[error("not implemented arrow datatype: {0}")]
    NotImplementedArrowDataType(String),
    #[error("not implemented sqlparser datatype: {0}")]
    NotImplementedSqlparserDataType(String),
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("cast fail")]
    CastFail,
}
