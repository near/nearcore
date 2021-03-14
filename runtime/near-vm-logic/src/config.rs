use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::hash::Hash;

#[derive(Clone, Copy, Debug, Hash, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum VMKind {
    /// Wasmer 0.17.x VM.
    Wasmer0,
    /// Wasmtime VM.
    Wasmtime,
    /// Wasmer 1.x VM.
    Wasmer1,
}

impl Default for VMKind {
    #[cfg(all(
        feature = "wasmer0_default",
        not(feature = "wasmer1_default"),
        not(feature = "wasmtime_default")
    ))]
    fn default() -> Self {
        VMKind::Wasmer0
    }

    #[cfg(all(
        not(feature = "wasmer0_default"),
        feature = "wasmer1_default",
        not(feature = "wasmtime_default")
    ))]
    fn default() -> Self {
        VMKind::Wasmer1
    }

    #[cfg(all(
        not(feature = "wasmer0_default"),
        not(feature = "wasmer1_default"),
        feature = "wasmtime_default"
    ))]
    fn default() -> Self {
        VMKind::Wasmtime
    }

    #[cfg(all(
        not(feature = "wasmer0_default"),
        not(feature = "wasmer1_default"),
        not(feature = "wasmtime_default")
    ))]
    fn default() -> Self {
        VMKind::Wasmer0
    }

    // These features should be mutually exclusive, but implement this to work around CI cargo check --all-features
    #[cfg(all(
        feature = "wasmer0_default",
        feature = "wasmer1_default",
        feature = "wasmtime_default"
    ))]
    fn default() -> Self {
        VMKind::Wasmer0
    }
}
