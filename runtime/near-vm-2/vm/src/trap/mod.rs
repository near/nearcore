// This file contains code from external sources.
// Attributions: https://github.com/wasmerio/wasmer/blob/2.3.0/ATTRIBUTIONS.md

//! This is the module that facilitates the usage of Traps
//! in Wasmer Runtime
mod trapcode;
pub mod traphandlers;

pub use trapcode::TrapCode;
pub use traphandlers::resume_panic;
pub use traphandlers::{
    TlsRestore, Trap, catch_traps, catch_traps_with_result, near_vm_call_trampoline,
    raise_lib_trap, raise_user_trap,
};
