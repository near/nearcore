use borsh::BorshSerialize;
use near_primitives::checked_feature;
use near_vm_logic::ProtocolVersion;
use std::hash::Hash;

#[derive(Clone, Copy, Debug, Hash, BorshSerialize)]
// Note, that VMKind is part of serialization protocol, so we cannor remove entries
// from this list if particular VM reached publically visible networks.
pub enum VMKind {
    /// Wasmer 0.17.x VM.
    Wasmer0,
    /// Wasmtime VM.
    Wasmtime,
    /// Wasmer 1.x VM, no longer supported.
    // Wasmer1,
    //  Wasmer 2.x VM,
    Wasmer2,
}

impl VMKind {
    pub fn for_protocol_version(protocol_version: ProtocolVersion) -> VMKind {
        if cfg!(feature = "force_wasmer0") {
            return VMKind::Wasmer0;
        }
        if cfg!(feature = "force_wasmtime") {
            return VMKind::Wasmtime;
        }
        if cfg!(feature = "force_wasmer2") {
            return VMKind::Wasmer2;
        }

        if checked_feature!("stable", Wasmer2, protocol_version) {
            VMKind::Wasmer2
        } else {
            VMKind::Wasmer0
        }
    }
}
