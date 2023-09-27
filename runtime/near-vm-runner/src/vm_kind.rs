use borsh::BorshSerialize;
use std::hash::Hash;

// Note, that VMKind is part of serialization protocol, so we cannot remove entries from this list
// if particular VM reached publicly visible networks.
//
// Additionally, this is public only for the purposes of internal tools like thea estimator. This
// API should otherwise be considered a private implementation detail of the `near-vm-runner`
// crate.
#[derive(
    Clone,
    Copy,
    Debug,
    Hash,
    BorshSerialize,
    PartialEq,
    Eq,
    strum::EnumString,
    serde::Serialize,
    serde::Deserialize,
)]
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
