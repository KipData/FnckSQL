#![feature(error_generic_member_access)]
#![feature(provide_any)]
#![allow(unused_doc_comments)]
#![feature(result_flattening)]
#![feature(generators)]
#![feature(iterator_try_collect)]
#![allow(cast_ref_to_mut)]
pub mod binder;
pub mod catalog;
pub mod db;
pub mod execution;
pub mod expression;
pub mod parser;
pub mod planner;
pub mod storage;
pub mod types;
pub mod util;
mod execution_v1;
mod optimizer;
