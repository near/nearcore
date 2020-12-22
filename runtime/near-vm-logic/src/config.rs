use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::hash::Hash;

#[derive(Clone, Copy, Debug, Hash, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum VMKind {
    /// Wasmer VM.
    Wasmer,
    /// Wasmtime VM.
    Wasmtime,
}

impl Default for VMKind {
    #[cfg(feature = "wasmer_default")]
    fn default() -> Self {
        VMKind::Wasmer
    }

    #[cfg(feature = "wasmtime_default")]
    fn default() -> Self {
        VMKind::Wasmtime
    }

    #[cfg(all(not(feature = "wasmer_default"), not(feature = "wasmtime_default")))]
    fn default() -> Self {
        VMKind::Wasmer
    }
}
