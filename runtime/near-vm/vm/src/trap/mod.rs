// This file contains code from external sources.
// Attributions: https://github.com/wasmerio/wasmer/blob/master/ATTRIBUTIONS.md

//! This is the module that facilitates the usage of Traps
//! in Wasmer Runtime
mod trapcode;
pub mod traphandlers;

pub use trapcode::TrapCode;
pub use traphandlers::resume_panic;
pub use traphandlers::{
    catch_traps, catch_traps_with_result, near_vm_call_trampoline, raise_lib_trap, raise_user_trap,
    TlsRestore, Trap,
};
