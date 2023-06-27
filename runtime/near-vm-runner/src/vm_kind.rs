use crate::logic::ProtocolVersion;
use borsh::BorshSerialize;
use near_primitives_core::checked_feature;
use std::hash::Hash;

// Note, that VMKind is part of serialization protocol, so we cannot remove entries from this list
// if particular VM reached publicly visible networks.
//
// Additionally, this is public only for the purposes of internal tools like thea estimator. This
// API should otherwise be considered a private implementation detail of the `near-vm-runner`
// crate.
#[derive(Clone, Copy, Debug, Hash, BorshSerialize, PartialEq, Eq)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum VMKind {
    /// Wasmer 0.17.x VM.
    Wasmer0,
    /// Wasmtime VM.
    Wasmtime,
    /// Wasmer 2.x VM.
    Wasmer2,
    /// NearVM.
    NearVm,
}

impl VMKind {
    pub fn for_protocol_version(protocol_version: ProtocolVersion) -> VMKind {
        // Only wasmtime supports non-x86_64 systems
        #[cfg(all(
            not(target_arch = "x86_64"),
            any(feature = "force_wasmer0", feature = "force_wasmer2")
        ))]
        compile_error!(
            "Wasmer only supports x86_64, but a force_wasmer* feature was passed to near-vm-runner"
        );

        if cfg!(feature = "force_wasmer0") {
            return VMKind::Wasmer0;
        }
        if cfg!(feature = "force_wasmtime") {
            return VMKind::Wasmtime;
        }
        if cfg!(feature = "force_wasmer2") {
            return VMKind::Wasmer2;
        }

        if cfg!(not(target_arch = "x86_64")) {
            return VMKind::Wasmtime;
        }
        if checked_feature!("stable", NearVmRuntime, protocol_version) {
            return VMKind::NearVm;
        }
        if checked_feature!("stable", Wasmer2, protocol_version) {
            return VMKind::Wasmer2;
        }
        return VMKind::Wasmer0;
    }
}
