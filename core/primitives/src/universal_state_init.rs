//! Typed initial state of a `0u` universal account.
//!
//! [`UniversalStateInit`] fully describes a universal account: contract code,
//! initial storage, and access keys. Its account id is the `0u` encoding of
//! SHA3-256 over the borsh of this value (see
//! [`crate::utils::derive_universal_account_id`]).
//!
//! The type is a flat, versioned struct rather than one variant per account
//! "kind": which fields are populated decides the kind (a key-only account has no
//! `code`; a contract account has `code`).
//!
//! The id commits to the exact bytes hashed. Only the canonical borsh (sorted
//! `BTree*` containers without duplicates, one struct per version) is accepted:
//! [`UniversalStateInit::from_raw`] rejects any encoding that is not byte for byte
//! what [`UniversalStateInit::to_raw`] produces. So each logical state init has
//! exactly one valid encoding and therefore one account id. Only the outer
//! structure is checked: contracts serialize their own nested state inside the
//! opaque storage values, out of the protocol's view.
//!
//! This is host-only (it embeds `near-crypto` key handles). The crypto-free wire
//! form [`RawStateInit`] lives in `near-primitives-core` and is re-exported here.

use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKeyHandle;
use near_primitives_core::global_contract::GlobalContractIdentifier;
pub use near_primitives_core::universal_state_init::{
    RawStateInit, StateInitError, UniversalStateInitCounts,
};
use near_schema_checker_lib::ProtocolSchema;
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::{BTreeMap, BTreeSet};

/// Versioned initial state of a `0u` universal account. New fields or semantics
/// arrive as a new variant (`V2`, ...); the discriminant is the only version marker.
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
#[borsh(use_discriminant = true)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum UniversalStateInit {
    V1(UniversalStateInitV1) = 0,
}

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
pub struct UniversalStateInitV1 {
    /// Contract code, or `None` for a key-only (EOA) account.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<GlobalContractIdentifier>,
    /// Initial storage; empty unless seeded. Sorted keys give a canonical encoding.
    #[serde_as(as = "BTreeMap<Base64, Base64>")]
    #[cfg_attr(feature = "schemars", schemars(with = "BTreeMap<String, String>"))]
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Full-access keys as compact on-trie handles. Sorted for a canonical encoding.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub access_keys: BTreeSet<PublicKeyHandle>,
}

impl UniversalStateInit {
    /// Contract code, or `None` for a key-only account.
    pub fn code(&self) -> Option<&GlobalContractIdentifier> {
        match self {
            UniversalStateInit::V1(inner) => inner.code.as_ref(),
        }
    }

    pub fn data(&self) -> &BTreeMap<Vec<u8>, Vec<u8>> {
        match self {
            UniversalStateInit::V1(inner) => &inner.data,
        }
    }

    pub fn access_keys(&self) -> &BTreeSet<PublicKeyHandle> {
        match self {
            UniversalStateInit::V1(inner) => &inner.access_keys,
        }
    }

    pub fn version(&self) -> u32 {
        match self {
            UniversalStateInit::V1(_) => 1,
        }
    }

    /// Canonical borsh of this state init, ready to wrap in a [`RawStateInit`].
    pub fn to_raw(&self) -> RawStateInit {
        RawStateInit(borsh::to_vec(self).expect("borsh must not fail"))
    }

    /// Decode `raw` into a typed value, accepting only the canonical encoding.
    ///
    /// Rejects malformed or trailing bytes, and any encoding that differs from [`Self::to_raw`]
    /// of the decoded value (unsorted or duplicate map keys and set elements).
    pub fn from_raw(raw: &RawStateInit) -> Result<Self, StateInitError> {
        let decoded = Self::try_from_slice(&raw.0).map_err(|_| StateInitError::Malformed)?;
        // Borsh's `de_strict_order` would probably catch this, but it is a crate-wide feature
        // that would also change how every other `BTreeMap` and `BTreeSet` in the node decodes.
        if decoded.to_raw() != *raw {
            return Err(StateInitError::NonCanonical);
        }
        Ok(decoded)
    }
}

/// The counts a `UniversalStateInit` action is priced on.
///
/// The byte count is the payload's own length, not that of the decoded state, so
/// it is what the host actually had to read and decode.
///
/// The entry and key counts come from [`UniversalStateInit::from_raw`], and bytes
/// it rejects (malformed or not canonical) describe no entries and no keys. That
/// arm is reachable and priced: a contract can hand
/// `promise_batch_action_universal_state_init` anything at all, and it pays for the
/// bytes it made the host read before new-receipt validation turns them into
/// `MalformedUniversalStateInit`. Base and per-byte is what that work amounts to.
///
/// Gas therefore depends on what is accepted, so a new [`UniversalStateInit`]
/// variant has to arrive behind a protocol feature: two nodes on one protocol
/// version that disagree about whether a payload is accepted would charge
/// different gas for it. Validity already carried that requirement, since
/// `validate_universal_state_init` rejects the same payloads; gas now shares it.
pub fn state_init_counts(raw: &RawStateInit) -> UniversalStateInitCounts {
    let (num_entries, num_keys) = match UniversalStateInit::from_raw(raw) {
        Ok(state_init) => (state_init.data().len() as u64, state_init.access_keys().len() as u64),
        Err(_) => (0, 0),
    };
    UniversalStateInitCounts { num_bytes: raw.0.len() as u64, num_entries, num_keys }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_crypto::{ED25519PublicKey, MlDsa65PublicKeyHandle};
    use near_primitives_core::hash::CryptoHash;

    fn ed_handle(b: u8) -> PublicKeyHandle {
        PublicKeyHandle::ED25519(ED25519PublicKey([b; 32]))
    }

    fn mldsa_handle(b: u8) -> PublicKeyHandle {
        PublicKeyHandle::MlDsa65(MlDsa65PublicKeyHandle([b; 32]))
    }

    fn contract_init() -> UniversalStateInit {
        let mut data = BTreeMap::new();
        data.insert(b"alpha".to_vec(), b"1".to_vec());
        data.insert(b"beta".to_vec(), b"2".to_vec());
        UniversalStateInit::V1(UniversalStateInitV1 {
            code: Some(GlobalContractIdentifier::CodeHash(CryptoHash::default())),
            data,
            access_keys: BTreeSet::from([ed_handle(7), mldsa_handle(9)]),
        })
    }

    fn key_only_init() -> UniversalStateInit {
        UniversalStateInit::V1(UniversalStateInitV1 {
            code: None,
            data: BTreeMap::new(),
            access_keys: BTreeSet::from([ed_handle(1)]),
        })
    }

    #[test]
    fn borsh_round_trip() {
        for si in [contract_init(), key_only_init()] {
            let raw = si.to_raw();
            assert_eq!(UniversalStateInit::try_from_slice(&raw.0).unwrap(), si);
            assert_eq!(UniversalStateInit::from_raw(&raw).unwrap(), si);
        }
    }

    #[test]
    fn accessors() {
        let c = contract_init();
        assert!(c.code().is_some());
        assert_eq!(c.data().len(), 2);
        assert_eq!(c.access_keys().len(), 2);
        assert_eq!(c.version(), 1);

        let k = key_only_init();
        assert!(k.code().is_none());
        assert!(k.data().is_empty());
        assert_eq!(k.access_keys().len(), 1);
    }

    #[test]
    fn serde_json_round_trip() {
        // Exercises the non-trivial base64 serde wiring the view/RPC layer will use.
        let si = contract_init();
        let json = serde_json::to_string(&si).unwrap();
        assert_eq!(serde_json::from_str::<UniversalStateInit>(&json).unwrap(), si);

        // A `RawStateInit` serializes as a single base64 JSON string.
        let raw = si.to_raw();
        let raw_json = serde_json::to_string(&raw).unwrap();
        assert!(raw_json.starts_with('"') && raw_json.ends_with('"'));
        assert_eq!(serde_json::from_str::<RawStateInit>(&raw_json).unwrap(), raw);
    }

    #[test]
    fn serde_json_shape() {
        // Externally tagged with a lowercase tag, and empty fields are omitted.
        let empty = UniversalStateInit::V1(UniversalStateInitV1 {
            code: None,
            data: BTreeMap::new(),
            access_keys: BTreeSet::new(),
        });
        assert_eq!(serde_json::to_string(&empty).unwrap(), r#"{"v1":{}}"#);
        assert_eq!(serde_json::from_str::<UniversalStateInit>(r#"{"v1":{}}"#).unwrap(), empty);

        // Populated fields still round-trip through the omit-on-default wiring.
        let si = key_only_init();
        let json = serde_json::to_string(&si).unwrap();
        assert!(json.starts_with(r#"{"v1":{"access_keys":"#), "unexpected shape: {json}");
        assert_eq!(serde_json::from_str::<UniversalStateInit>(&json).unwrap(), si);
    }

    #[test]
    fn from_raw_rejects_trailing_bytes() {
        let mut bytes = contract_init().to_raw().0;
        bytes.push(0);
        assert_eq!(
            UniversalStateInit::from_raw(&RawStateInit(bytes)),
            Err(StateInitError::Malformed)
        );
    }

    #[test]
    fn from_raw_rejects_malformed() {
        // Empty input and a truncated body (discriminant only) fail borsh decoding.
        for bytes in [vec![], vec![0u8]] {
            assert_eq!(
                UniversalStateInit::from_raw(&RawStateInit(bytes)),
                Err(StateInitError::Malformed)
            );
        }
    }

    /// Borsh bytes of `V1{code: None, data: <entries as given>, access_keys: <keys as given>}`.
    fn v1_bytes(data: &[(&[u8], &[u8])], access_keys: &[PublicKeyHandle]) -> RawStateInit {
        let mut bytes = vec![0u8, 0u8]; // V1 discriminant, code: None
        bytes.extend_from_slice(&(data.len() as u32).to_le_bytes());
        for (key, value) in data {
            bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
            bytes.extend_from_slice(key);
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value);
        }
        bytes.extend_from_slice(&(access_keys.len() as u32).to_le_bytes());
        for handle in access_keys {
            bytes.extend(borsh::to_vec(handle).unwrap());
        }
        RawStateInit(bytes)
    }

    #[test]
    fn from_raw_accepts_hand_built_canonical() {
        let raw = v1_bytes(&[(b"a", b"1"), (b"b", b"2")], &[ed_handle(1), ed_handle(2)]);
        let decoded = UniversalStateInit::from_raw(&raw).unwrap();
        assert_eq!(decoded.to_raw(), raw);
    }

    #[test]
    fn from_raw_rejects_unsorted_data() {
        let raw = v1_bytes(&[(b"b", b""), (b"a", b"")], &[]);
        assert_eq!(UniversalStateInit::from_raw(&raw), Err(StateInitError::NonCanonical));
    }

    #[test]
    fn from_raw_rejects_unsorted_access_keys() {
        let raw = v1_bytes(&[], &[ed_handle(2), ed_handle(1)]);
        assert_eq!(UniversalStateInit::from_raw(&raw), Err(StateInitError::NonCanonical));
    }

    #[test]
    fn from_raw_rejects_duplicates() {
        let raw = v1_bytes(&[(b"a", b"1"), (b"a", b"2")], &[]);
        assert_eq!(UniversalStateInit::from_raw(&raw), Err(StateInitError::NonCanonical));
        let raw = v1_bytes(&[], &[ed_handle(1), ed_handle(1)]);
        assert_eq!(UniversalStateInit::from_raw(&raw), Err(StateInitError::NonCanonical));
    }
}
