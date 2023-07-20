#![feature(error_generic_member_access)]
#![feature(provide_any)]
#![allow(unused_doc_comments)]
// #![deny(
// // The following are allowed by default lints according to
// // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
// absolute_paths_not_starting_with_crate,
// // box_pointers, async trait must use it
// // elided_lifetimes_in_paths, // allow anonymous lifetime
// explicit_outlives_requirements,
// keyword_idents,
// macro_use_extern_crate,
// meta_variable_misuse,
// missing_abi,
// missing_copy_implementations,
// // must_not_suspend, unstable
// non_ascii_idents,
// // non_exhaustive_omitted_patterns, unstable
// noop_method_call,
// pointer_structural_match,
// rust_2021_incompatible_closure_captures,
// rust_2021_incompatible_or_patterns,
// rust_2021_prefixes_incompatible_syntax,
// rust_2021_prelude_collisions,
// single_use_lifetimes,
// trivial_casts,
// trivial_numeric_casts,
// //unreachable_pub,
// unsafe_op_in_unsafe_fn,
// // unstable_features,
// // unused_crate_dependencies, the false positive case blocks us
// unused_import_braces,
// unused_lifetimes,
// unused_qualifications,
// //unused_results,
// //warnings, // treat all wanings as errors
// clippy::all,
// clippy::cargo,
// // The followings are selected restriction lints for rust 1.57
// clippy::clone_on_ref_ptr,
// clippy::create_dir,
// clippy::dbg_macro,
// clippy::decimal_literal_representation,
// // clippy::default_numeric_fallback, too verbose when dealing with numbers
// clippy::disallowed_script_idents,
// clippy::else_if_without_else,
// clippy::exit,
// clippy::expect_used,
// clippy::filetype_is_file,
// clippy::float_arithmetic,
// clippy::float_cmp_const,
// clippy::get_unwrap,
// clippy::if_then_some_else_none,
// // clippy::implicit_return, it's idiomatic Rust code.
// // clippy::inline_asm_x86_att_syntax, stick to intel syntax
// clippy::inline_asm_x86_intel_syntax,
// // clippy::integer_division, required in the project
// clippy::let_underscore_must_use,
// clippy::lossy_float_literal,
// clippy::mem_forget,
// clippy::missing_enforced_import_renames,
// clippy::missing_inline_in_public_items,
// // clippy::mod_module_files, mod.rs file is used
// clippy::modulo_arithmetic,
// clippy::multiple_inherent_impl,
// // clippy::panic, allow in application code
// // clippy::panic_in_result_fn, not necessary as panic is banned
// clippy::print_stderr,
// clippy::print_stdout,
// clippy::rc_mutex,
// clippy::rest_pat_in_fully_bound_structs,
// clippy::same_name_method,
// clippy::self_named_module_files,
// // clippy::shadow_reuse, it’s a common pattern in Rust code
// // clippy::shadow_same, it’s a common pattern in Rust code
// clippy::str_to_string,
// clippy::string_add,
// clippy::string_to_string,
// clippy::todo,
// clippy::unimplemented,
// clippy::unnecessary_self_imports,
// clippy::unneeded_field_pattern,
// // clippy::unreachable, allow unreachable panic, which is out of expectation
// clippy::unwrap_in_result,
// clippy::unwrap_used,
// // clippy::use_debug, debug is allow for debug log
// clippy::verbose_file_reads,
// )]
// #![allow(
// clippy::panic, // allow debug_assert, panic in production code
// clippy::multiple_crate_versions, // caused by the dependency, can't be fixed
// )]
#![feature(result_flattening)]
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
