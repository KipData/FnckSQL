#![feature(error_generic_member_access)]
#![allow(unused_doc_comments)]
#![feature(result_flattening)]
#![feature(coroutines)]
#![feature(iterator_try_collect)]
#![feature(slice_pattern)]
#![feature(is_sorted)]
extern crate core;
pub mod binder;
pub mod catalog;
pub mod db;
pub mod errors;
pub mod execution;
pub mod expression;
#[cfg(feature = "marcos")]
pub mod marcos;
mod optimizer;
pub mod parser;
pub mod planner;
pub mod storage;
pub mod types;
mod udf;
