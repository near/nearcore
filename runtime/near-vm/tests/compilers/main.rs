//! This test suite does all the tests that involve any compiler
//! implementation, such as: singlepass.

#[macro_use]
extern crate near_vm_compiler_test_derive;

mod config;
mod deterministic;
mod imports;
mod issues;
// mod multi_value_imports;
mod compilation;
mod native_functions;
mod serialize;
mod stack_limiter;
mod traps;
mod wast;

pub use crate::config::{Compiler, Config, Engine};
pub use crate::wast::run_wast;
