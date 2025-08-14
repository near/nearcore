// cspell:ignore waitlist

#![doc = include_str!("../README.md")]
#![cfg_attr(enable_const_type_id, feature(const_type_id))]

mod cache;
mod errors;
mod features;
mod imports;
pub mod logic;
#[cfg(feature = "metrics")]
mod metrics;
#[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
mod near_vm_2_runner;
#[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
mod near_vm_runner;
#[cfg(feature = "prepare")]
pub mod prepare;
mod profile;
mod runner;
#[cfg(test)]
mod tests;
mod utils;
#[cfg(feature = "wasmtime_vm")]
mod wasmtime_runner;

pub use crate::logic::with_ext_cost_counter;
#[cfg(not(windows))]
pub use cache::FilesystemContractRuntimeCache;
pub use cache::{
    CompiledContract, CompiledContractInfo, ContractRuntimeCache, MockContractRuntimeCache,
    NoContractRuntimeCache, get_contract_cache_key, precompile_contract,
};
#[cfg(feature = "metrics")]
pub use metrics::{report_metrics, reset_metrics};
pub use near_primitives_core::code::ContractCode;
pub use profile::ProfileDataV3;
pub use runner::{Contract, PreparedContract, VM, prepare, run};

#[cfg(any(feature = "prepare", feature = "wasmtime_vm"))]
pub(crate) const MEMORY_EXPORT: &str = "\0nearcore_memory";

/// This is public for internal experimentation use only, and should otherwise be considered an
/// implementation detail of `near-vm-runner`.
#[doc(hidden)]
pub mod internal {
    pub use crate::runner::VMKindExt;
    #[cfg(feature = "prepare")]
    pub use wasmparser;
}

/// Drop something somewhat lazily.
///
/// The memory destruction is sorta expensive process, but not expensive enough to offload it into
/// a thread for individual instances.
///
/// Instead this method will gather up a number of things before initiating a release in a thread,
/// thus working in batches of sorts and amortizing the thread overhead.
#[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
pub(crate) fn lazy_drop(what: Box<dyn std::any::Any + Send>) {
    // TODO: this would benefit from a lock-free array (should be straightforward enough to
    // implement too...) But for the time being this mutex is not really contended much so…
    // whatever.
    const CHUNK_SIZE: usize = 8;
    static WAITLIST: std::sync::OnceLock<parking_lot::Mutex<Vec<Box<dyn std::any::Any + Send>>>> =
        std::sync::OnceLock::new();
    let waitlist = WAITLIST.get_or_init(|| parking_lot::Mutex::new(Vec::with_capacity(CHUNK_SIZE)));
    let mut waitlist = waitlist.lock();
    if waitlist.capacity() > waitlist.len() {
        waitlist.push(Box::new(what));
    }
    if waitlist.capacity() == waitlist.len() {
        let chunk = std::mem::replace(&mut *waitlist, Vec::with_capacity(CHUNK_SIZE));
        rayon::spawn(move || drop(chunk));
    }
}
