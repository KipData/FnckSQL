#![feature(error_generic_member_access)]
#![allow(unused_doc_comments)]
#![feature(result_flattening)]
#![feature(generators)]
#![feature(iterator_try_collect)]
#![feature(slice_pattern)]
#![feature(bound_map)]
extern crate core;

pub mod binder;
pub mod catalog;
pub mod db;
pub mod expression;
pub mod parser;
pub mod planner;
pub mod types;
mod optimizer;
pub mod execution;
pub mod storage;
