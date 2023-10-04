use chrono::ParseError;
use std::num::{ParseFloatError, ParseIntError, TryFromIntError};
use std::str::ParseBoolError;
use std::string::FromUtf8Error;

#[derive(thiserror::Error, Debug)]
pub enum TypeError {
    #[error("invalid type")]
    InvalidType,
    #[error("Must contain PrimaryKey!")]
    PrimaryKeyNotFound,
    #[error("not implemented sqlparser datatype: {0}")]
    NotImplementedSqlparserDataType(String),
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("cast fail")]
    CastFail,
    #[error("Too long")]
    TooLong,
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
    #[error("bindcode")]
    Bincode(
        #[source]
        #[from]
        Box<bincode::ErrorKind>,
    ),
    #[error("try from decimal")]
    TryFromDecimal(
        #[source]
        #[from]
        rust_decimal::Error,
    ),
    #[error("from utf8")]
    FromUtf8Error(
        #[source]
        #[from]
        FromUtf8Error,
    ),
}
