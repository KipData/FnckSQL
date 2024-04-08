use crate::expression::BinaryOperator;
use crate::types::LogicalType;
use chrono::ParseError;
use kip_db::KernelError;
use sqlparser::parser::ParserError;
use std::num::{ParseFloatError, ParseIntError, TryFromIntError};
use std::str::ParseBoolError;
use std::string::FromUtf8Error;

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("agg miss: {0}")]
    AggMiss(String),
    #[error("bindcode: {0}")]
    Bincode(
        #[source]
        #[from]
        Box<bincode::ErrorKind>,
    ),
    #[error("cast fail")]
    CastFail,
    #[error("channel close")]
    ChannelClose,
    #[error("columns empty")]
    ColumnsEmpty,
    #[error("csv error: {0}")]
    Csv(
        #[from]
        #[source]
        csv::Error,
    ),
    #[error("default does not exist")]
    DefaultNotExist,
    #[error("column: {0} already exists")]
    DuplicateColumn(String),
    #[error("index: {0} already exists")]
    DuplicateIndex(String),
    #[error("duplicate primary key")]
    DuplicatePrimaryKey,
    #[error("the column has been declared unique and the value already exists")]
    DuplicateUniqueValue,
    #[error("empty plan")]
    EmptyPlan,
    #[error("sql statement is empty")]
    EmptyStatement,
    #[error("evaluator not found")]
    EvaluatorNotFound,
    #[error("from utf8: {0}")]
    FromUtf8Error(
        #[source]
        #[from]
        FromUtf8Error,
    ),
    #[error("can not compare two types: {0} and {1}")]
    Incomparable(LogicalType, LogicalType),
    #[error("invalid column: {0}")]
    InvalidColumn(String),
    #[error("invalid index")]
    InvalidIndex,
    #[error("invalid table: {0}")]
    InvalidTable(String),
    #[error("invalid type")]
    InvalidType,
    #[error("io: {0}")]
    IO(
        #[source]
        #[from]
        std::io::Error,
    ),
    #[error("task join error: {0}")]
    TaskJoinError(
        #[from]
        #[source]
        tokio::task::JoinError,
    ),
    #[error("kipdb error: {0}")]
    KipDBError(
        #[source]
        #[from]
        KernelError,
    ),
    #[error("{0} and {1} do not match")]
    MisMatch(&'static str, &'static str),
    #[error("add column must be nullable or specify a default value")]
    NeedNullAbleOrDefault,
    #[error("no transaction begin")]
    NoTransactionBegin,
    #[error("cannot be Null")]
    NotNull,
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("parser bool: {0}")]
    ParseBool(
        #[source]
        #[from]
        ParseBoolError,
    ),
    #[error("parser date: {0}")]
    ParseDate(
        #[source]
        #[from]
        ParseError,
    ),
    #[error("parser float: {0}")]
    ParseFloat(
        #[source]
        #[from]
        ParseFloatError,
    ),
    #[error("parser int: {0}")]
    ParseInt(
        #[source]
        #[from]
        ParseIntError,
    ),
    #[error("parser sql: {0}")]
    ParserSql(
        #[source]
        #[from]
        ParserError,
    ),
    #[error("must contain primary key!")]
    PrimaryKeyNotFound,
    #[error("the table already exists")]
    TableExists,
    #[error("the table not found")]
    TableNotFound,
    #[error("transaction already exists")]
    TransactionAlreadyExists,
    #[error("try from decimal: {0}")]
    TryFromDecimal(
        #[source]
        #[from]
        rust_decimal::Error,
    ),
    #[error("try from int: {0}")]
    TryFromInt(
        #[source]
        #[from]
        TryFromIntError,
    ),
    #[error("too long")]
    TooLong,
    #[error("there are more buckets: {0} than elements: {1}")]
    TooManyBuckets(usize, usize),
    #[error("unsupported binary operator: {0} cannot support {1} for calculations")]
    UnsupportedBinaryOperator(LogicalType, BinaryOperator),
    #[error("unsupported statement: {0}")]
    UnsupportedStmt(String),
    #[error("values length not match, expect {0}, got {1}")]
    ValuesLenMismatch(usize, usize),
}
