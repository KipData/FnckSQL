#[derive(thiserror::Error, Debug)]
pub enum TypeError {
    #[error("invalid type")]
    InvalidType,
    #[error("not implemented sqlparser datatype: {0}")]
    NotImplementedSqlparserDataType(String),
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("cast fail")]
    CastFail,
    #[error("Cannot be Null")]
    NotNull,
}
