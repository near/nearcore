//! Opaque wire form of a universal-account StateInit.
//!
//! [`RawStateInit`] carries the canonical borsh of a `UniversalStateInit` (the
//! typed form lives in `near-primitives`, which decodes and validates it). It sits
//! in this crypto-free core crate because the host-function ABI in `near-vm-runner`
//! will name it while forwarding the bytes verbatim; decoding into the typed form
//! is a host-side concern.

use borsh::{BorshDeserialize, BorshSerialize};
use near_schema_checker_lib::ProtocolSchema;
use serde_with::base64::Base64;
use serde_with::serde_as;

/// Canonical borsh serialization of a universal-account StateInit, forwarded opaquely.
///
/// It lets an immutable contract pass through a StateInit version it predates: the
/// bytes travel verbatim, so a version added after the contract was compiled still
/// works. Because the account id hashes these exact bytes, they must be canonical
/// borsh; `near_primitives` decodes them with that check.
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
