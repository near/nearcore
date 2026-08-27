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
/// never re-serialized to derive an ID. Two encodings of the same logical value
/// are two different accounts, which is deliberate: canonical encoding cannot be
/// enforced end to end anyway, since contracts serialize their own nested state
/// inside the opaque storage values.
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
