//! Opaque wire form of a universal-account StateInit.
//!
//! [`RawStateInit`] carries the borsh of a `UniversalStateInit` (the typed form
//! lives in `near-primitives`, which decodes it). It sits in this crypto-free core
//! crate because the host-function ABI in `near-vm-runner` will name it while
//! forwarding the bytes verbatim; decoding into the typed form is a host-side concern.

use serde_with::base64::Base64;
use serde_with::serde_as;

/// Borsh serialization of a universal-account StateInit, forwarded opaquely.
///
/// It lets an immutable contract pass through a StateInit version it predates: the
/// bytes travel verbatim, so a version added after the contract was compiled still
/// works. The account id hashes these exact bytes.
///
/// Deliberately not `BorshSerialize`/`BorshDeserialize`: the payload is already
/// borsh, and borsh-serializing this newtype would prepend a 4-byte `Vec` length,
/// so hashing the wrapper instead of the inner bytes would derive a wrong id. The
/// bytes cross the host-function ABI verbatim and serialize as base64 over serde.
#[serde_as]
#[derive(PartialEq, Eq, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RawStateInit(
    #[serde_as(as = "Base64")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub Vec<u8>,
);
