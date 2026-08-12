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
//! The id commits to the exact bytes hashed. The typed API always produces the
//! canonical borsh (sorted `BTree*` containers, one struct per version), so ids
//! minted through it are stable. Decoding, however, does not reject a
//! non-canonical encoding of the same logical value: such an encoding simply
//! hashes to a different id, and canonicalization cannot be enforced end to end
//! anyway (contracts re-serialize their own nested `BTree*` state inside opaque
//! storage values, out of the protocol's view). Producers are responsible for
//! serializing consistently.
//!
//! This is host-only (it embeds `near-crypto` key handles). The crypto-free wire
//! form [`RawStateInit`] lives in `near-primitives-core` and is re-exported here.

use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKeyHandle;
use near_primitives_core::deterministic_account_id::state_init_data_len_bytes;
use near_primitives_core::global_contract::GlobalContractIdentifier;
pub use near_primitives_core::universal_state_init::RawStateInit;
use near_schema_checker_lib::ProtocolSchema;
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::{BTreeMap, BTreeSet};
use std::io;

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

    /// Summed length of all storage keys and values, in bytes. Multiplied by
    /// the per-byte state-init fee, matching the deterministic-account rule.
    pub fn len_bytes(&self) -> usize {
        state_init_data_len_bytes(self.data())
    }

    /// Take the fields without cloning:
    /// `let (code, data, access_keys) = state_init.take();`.
    #[allow(clippy::type_complexity)]
    pub fn take(
        self,
    ) -> (Option<GlobalContractIdentifier>, BTreeMap<Vec<u8>, Vec<u8>>, BTreeSet<PublicKeyHandle>)
    {
        match self {
            UniversalStateInit::V1(inner) => (inner.code, inner.data, inner.access_keys),
        }
    }

    /// Canonical borsh of this state init, ready to wrap in a [`RawStateInit`].
    pub fn to_raw(&self) -> RawStateInit {
        RawStateInit(borsh::to_vec(self).expect("borsh must not fail"))
    }

    /// Decode `raw` into a typed value. Accepts any well-formed borsh encoding,
    /// rejecting only trailing or malformed bytes; a non-canonical encoding of
    /// the same logical value is accepted (it hashes to a different id). Callers
    /// that need a stable id should mint it through the typed `to_raw` / `derive`
    /// path, which always serializes canonically.
    pub fn from_raw(raw: &RawStateInit) -> Result<Self, io::Error> {
        Self::try_from_slice(&raw.0)
    }
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
        assert!(UniversalStateInit::from_raw(&RawStateInit(bytes)).is_err());
    }

    #[test]
    fn from_raw_rejects_malformed() {
        // Empty input and a truncated body (discriminant only) fail borsh decoding.
        for bytes in [vec![], vec![0u8]] {
            assert!(UniversalStateInit::from_raw(&RawStateInit(bytes)).is_err());
        }
    }

    #[test]
    fn from_raw_accepts_non_canonical_order() {
        // Hand-build V1{code: None, data: {"b","a"} out of order, access_keys: {}}.
        // borsh writes map keys sorted, so this is a non-canonical encoding. It is
        // now accepted: borsh silently re-sorts on read (`de_strict_order` is off)
        // and we no longer reject non-canonical encodings. Re-serializing yields
        // the canonical bytes, which differ, so this input hashes to a different id.
        let mut bytes = vec![0u8]; // V1 discriminant
        bytes.push(0u8); // code: None
        bytes.extend_from_slice(&2u32.to_le_bytes()); // data: 2 entries
        for key in [b'b', b'a'] {
            bytes.extend_from_slice(&1u32.to_le_bytes()); // key len
            bytes.push(key);
            bytes.extend_from_slice(&0u32.to_le_bytes()); // value len 0
        }
        bytes.extend_from_slice(&0u32.to_le_bytes()); // access_keys: empty

        let decoded = UniversalStateInit::from_raw(&RawStateInit(bytes.clone())).unwrap();
        assert_eq!(
            decoded.data().keys().cloned().collect::<Vec<_>>(),
            vec![b"a".to_vec(), b"b".to_vec()],
        );
        assert_ne!(decoded.to_raw().0, bytes, "canonical re-encoding must differ from the input");
    }

    #[test]
    fn from_raw_accepts_unsorted_access_keys() {
        // Two access keys serialized in reverse `Ord` order; borsh re-sorts the set
        // on read. Accepted, no longer rejected.
        let lo = borsh::to_vec(&ed_handle(1)).unwrap();
        let hi = borsh::to_vec(&ed_handle(2)).unwrap();
        let mut bytes = vec![0u8, 0u8]; // V1, code: None
        bytes.extend_from_slice(&0u32.to_le_bytes()); // data: empty
        bytes.extend_from_slice(&2u32.to_le_bytes()); // access_keys: 2 entries
        bytes.extend_from_slice(&hi); // reversed: handle(2) before handle(1)
        bytes.extend_from_slice(&lo);
        let decoded = UniversalStateInit::from_raw(&RawStateInit(bytes)).unwrap();
        assert_eq!(decoded.access_keys().len(), 2);
    }

    #[test]
    fn from_raw_accepts_duplicate_keys() {
        // A data map that declares 2 entries but repeats one key; borsh deduplicates
        // on read. Accepted; the decoded map has a single entry.
        let mut bytes = vec![0u8, 0u8]; // V1, code: None
        bytes.extend_from_slice(&2u32.to_le_bytes()); // data: 2 entries
        for _ in 0..2 {
            bytes.extend_from_slice(&1u32.to_le_bytes());
            bytes.push(b'a');
            bytes.extend_from_slice(&0u32.to_le_bytes());
        }
        bytes.extend_from_slice(&0u32.to_le_bytes()); // access_keys: empty
        let decoded = UniversalStateInit::from_raw(&RawStateInit(bytes)).unwrap();
        assert_eq!(decoded.data().len(), 1);
    }
}
