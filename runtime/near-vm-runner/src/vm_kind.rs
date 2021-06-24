use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::hash::Hash;

#[derive(Clone, Copy, Debug, Hash, Serialize, BorshSerialize)]
pub enum VMKind {
    /// Wasmer 0.17.x VM.
    Wasmer0 = 0,
    /// Wasmtime VM.
    Wasmtime = 1,
    /// Wasmer 1.x VM.
    // Wasmer1 = 2,
    //  Wasmer 2.x VM,
    Wasmer2 = 3,
}

impl Default for VMKind {
    #[cfg(all(
        feature = "wasmer0_default",
        not(feature = "wasmer2_default"),
        not(feature = "wasmtime_default")
    ))]
    fn default() -> Self {
        VMKind::Wasmer0
    }

    #[cfg(all(
        not(feature = "wasmer0_default"),
        feature = "wasmer2_default",
        not(feature = "wasmtime_default")
    ))]
    fn default() -> Self {
        VMKind::Wasmer2
    }

    #[cfg(all(
        not(feature = "wasmer0_default"),
        not(feature = "wasmer2_default"),
        feature = "wasmtime_default"
    ))]
    fn default() -> Self {
        VMKind::Wasmtime
    }

    #[cfg(all(
        not(feature = "wasmer0_default"),
        not(feature = "wasmer2_default"),
        not(feature = "wasmtime_default")
    ))]
    fn default() -> Self {
        VMKind::Wasmer0
    }

    // These features should be mutually exclusive, but implement this to work around CI cargo check --all-features
    #[cfg(all(
        feature = "wasmer0_default",
        feature = "wasmer2_default",
        feature = "wasmtime_default"
    ))]
    fn default() -> Self {
        VMKind::Wasmer0
    }
}
