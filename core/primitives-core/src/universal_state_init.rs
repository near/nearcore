//! Wire form of a universal-account StateInit.
//!
//! [`RawStateInit`] carries the borsh of a `UniversalStateInit` (the typed form
//! lives in `near-primitives`, which decodes it). It sits in this crypto-free core
//! crate because the host-function ABI in `near-vm-runner` names it while
//! forwarding the bytes verbatim; decoding into the typed form is a host-side concern.

use borsh::{BorshDeserialize, BorshSerialize};
use near_schema_checker_lib::ProtocolSchema;
use serde_with::base64::Base64;
use serde_with::serde_as;

/// Raw bytes containing borsh-serialized `UniversalStateInit`.
///
/// This is the protocol's view of a state init, not a mere transport wrapper: the
/// account ID is SHA3-256 over exactly these bytes. The typed form is a decoded
/// *view* of them, used to install the state and to price the action, and it is
/// never re-serialized to derive an ID. From `CanonicalUniversalStateInit` on, only
/// the canonical encoding is accepted (see `UniversalStateInit::from_raw_strict`), so
/// each logical value has one ID.
///
/// It also lets an immutable contract pass through a `UniversalStateInit` version it
/// predates: the bytes travel verbatim, so a version added after the contract was
/// compiled still works.
///
/// Borsh-serializing `RawStateInit` writes a 4-byte length prefix before the
/// bytes, which is how the `UniversalStateInit` action carries it as a field;
/// over serde the bytes are base64. Neither is what the account ID hashes: that
/// is `self.0` alone, never `borsh::to_vec(self)`.
#[serde_as]
#[derive(
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RawStateInit(
    #[serde_as(as = "Base64")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub Vec<u8>,
);

/// Why bytes are not a valid [`RawStateInit`] payload.
///
/// Shared with `near-vm-runner`, which reports it to contracts as a status code.
#[derive(PartialEq, Eq, Debug, Clone, Copy, thiserror::Error)]
pub enum StateInitError {
    /// The bytes do not decode into a `UniversalStateInit`.
    #[error("bytes do not decode into a universal state init")]
    Malformed,
    /// The bytes decode, but are not the canonical borsh encoding of the result.
    #[error("state init is not in canonical borsh form")]
    NonCanonical,
}

impl StateInitError {
    /// The exit status `universal_state_init_to_account_id` reports for this error.
    /// Success is `0`.
    pub const fn exit_status(self) -> u64 {
        match self {
            StateInitError::Malformed => 1,
            StateInitError::NonCanonical => 2,
        }
    }
}

/// How strictly the bytes of a [`RawStateInit`] are decoded.
///
/// Which one applies is a protocol decision: `Strict` from the
/// `CanonicalUniversalStateInit` protocol feature on, `Lenient` before it.
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum DecodeMode {
    /// Any well-formed borsh encoding is accepted.
    Lenient,
    /// Only the canonical borsh encoding is accepted.
    Strict,
}

impl DecodeMode {
    /// `Strict` if `strict`, else `Lenient`.
    pub const fn strict_if(strict: bool) -> Self {
        if strict { DecodeMode::Strict } else { DecodeMode::Lenient }
    }
}

/// What a `UniversalStateInit` action is priced on.
///
/// Shared with `near-vm-runner`: a contract hands the host opaque bytes, only the
/// host can decode them, and the VM needs these numbers to charge at creation
/// exactly what the action is charged when it executes.
#[derive(PartialEq, Eq, Debug, Clone, Copy, Default)]
pub struct UniversalStateInitCounts {
    /// Length of the payload itself, not of the state it decodes to.
    pub num_bytes: u64,
    /// Number of storage entries.
    pub num_entries: u64,
    /// Number of access keys.
    pub num_keys: u64,
}
