use std::num::{ParseFloatError, ParseIntError, TryFromIntError};
use std::str::ParseBoolError;
use chrono::ParseError;

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
    #[error("cannot be Null")]
    NotNull,
    #[error("try from int")]
    TryFromInt(
        #[source]
        #[from]
        TryFromIntError,
    ),
    #[error("parser int")]
    ParseInt(
        #[source]
        #[from]
        ParseIntError,
    ),
    #[error("parser bool")]
    ParseBool(
        #[source]
        #[from]
        ParseBoolError,
    ),
    #[error("parser float")]
    ParseFloat(
        #[source]
        #[from]
        ParseFloatError,
    ),
    #[error("parser date")]
    ParseDate(
        #[source]
        #[from]
        ParseError,
    ),
}
