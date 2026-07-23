//! Typed initial state of a `0u` universal account.
//!
//! [`UniversalStateInit`] fully describes a universal account: contract code,
//! initial storage, and access keys. Its account id is the `0u` encoding of
//! SHA3-256 over the canonical borsh of this value (see
//! [`crate::utils::derive_universal_account_id`]).
//!
//! The type is a flat, versioned struct rather than one variant per account
//! "kind": which fields are populated decides the kind (a key-only account has no
//! `code`; a contract account has `code`). Because the id hashes the exact bytes,
//! the encoding must be canonical: sorted containers and one struct per version
//! give exactly one byte string per end-state, so the id <-> bytes mapping is
//! bijective and cross-contract verification is unambiguous.
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
    pub code: Option<GlobalContractIdentifier>,
    /// Initial storage; empty unless seeded. Sorted keys give a canonical encoding.
    #[serde_as(as = "BTreeMap<Base64, Base64>")]
    #[cfg_attr(feature = "schemars", schemars(with = "BTreeMap<String, String>"))]
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
    /// Full-access keys as compact on-trie handles. Sorted for a canonical encoding.
    pub access_keys: BTreeSet<PublicKeyHandle>,
}

/// Reason a [`UniversalStateInit`] is not a usable account state.
///
/// The flat struct can express unusable field combinations, so validity is a
/// runtime invariant rather than a type-level guarantee.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum InvalidUniversalStateInit {
    #[error("universal state init defines neither contract code nor access keys")]
    NoCodeNoKeys,
}

/// Reason a [`RawStateInit`]'s bytes are not an acceptable [`UniversalStateInit`].
#[derive(thiserror::Error, Debug)]
pub enum ParseRawStateInitError {
    /// Bytes are not valid borsh for a `UniversalStateInit`, or have trailing data.
    #[error("invalid borsh: {0}")]
    Borsh(#[from] io::Error),
    /// Bytes decode but are not the unique canonical encoding (e.g. unsorted
    /// containers), which would break the id <-> bytes bijection.
    #[error("non-canonical state init encoding")]
    NonCanonical,
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

    /// Reject unusable states: an init with neither code nor an access key can
    /// never be controlled or run, so it must not create an account.
    pub fn validate(&self) -> Result<(), InvalidUniversalStateInit> {
        if self.code().is_none() && self.access_keys().is_empty() {
            return Err(InvalidUniversalStateInit::NoCodeNoKeys);
        }
        Ok(())
    }

    /// Canonical borsh of this state init, ready to wrap in a [`RawStateInit`].
    pub fn to_raw(&self) -> RawStateInit {
        RawStateInit(borsh::to_vec(self).expect("borsh must not fail"))
    }

    /// Decode `raw`, rejecting any non-canonical encoding so that the same account
    /// id always maps back to the same bytes. `try_from_slice` rejects trailing
    /// bytes; re-serializing and comparing rejects anything else that decodes but
    /// isn't the canonical form (e.g. unsorted map keys), independently of how
    /// strict the borsh reader happens to be.
    pub fn from_raw(raw: &RawStateInit) -> Result<Self, ParseRawStateInitError> {
        let value = Self::try_from_slice(&raw.0)?;
        if borsh::to_vec(&value).expect("borsh must not fail") != raw.0 {
            return Err(ParseRawStateInitError::NonCanonical);
        }
        Ok(value)
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
    fn validate_rejects_unusable() {
        // Neither code nor keys: unusable even with data present.
        let no_code_no_keys = UniversalStateInit::V1(UniversalStateInitV1 {
            code: None,
            data: BTreeMap::new(),
            access_keys: BTreeSet::new(),
        });
        assert_eq!(no_code_no_keys.validate(), Err(InvalidUniversalStateInit::NoCodeNoKeys));

        let data_only = UniversalStateInit::V1(UniversalStateInitV1 {
            code: None,
            data: BTreeMap::from([(b"x".to_vec(), b"y".to_vec())]),
            access_keys: BTreeSet::new(),
        });
        assert_eq!(data_only.validate(), Err(InvalidUniversalStateInit::NoCodeNoKeys));

        // A contract account and a key-only account are both usable.
        assert!(contract_init().validate().is_ok());
        assert!(key_only_init().validate().is_ok());

        // A pure contract account (code, but no keys and no data) is usable.
        let code_only = UniversalStateInit::V1(UniversalStateInitV1 {
            code: Some(GlobalContractIdentifier::CodeHash(CryptoHash::default())),
            data: BTreeMap::new(),
            access_keys: BTreeSet::new(),
        });
        assert!(code_only.validate().is_ok());
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
    fn from_raw_rejects_trailing_bytes() {
        let mut bytes = contract_init().to_raw().0;
        bytes.push(0);
        let err = UniversalStateInit::from_raw(&RawStateInit(bytes)).unwrap_err();
        assert!(matches!(err, ParseRawStateInitError::Borsh(_)), "got {err:?}");
    }

    #[test]
    fn from_raw_rejects_malformed() {
        // Empty input and a truncated body (discriminant only) fail borsh decoding.
        for bytes in [vec![], vec![0u8]] {
            let err = UniversalStateInit::from_raw(&RawStateInit(bytes)).unwrap_err();
            assert!(matches!(err, ParseRawStateInitError::Borsh(_)), "got {err:?}");
        }
    }

    #[test]
    fn from_raw_rejects_non_canonical_order() {
        // Hand-build V1{code: None, data: {"b","a"} out of order, access_keys: {}}.
        // borsh writes map keys sorted, so the canonical form is "a" then "b".
        let mut bytes = vec![0u8]; // V1 discriminant
        bytes.push(0u8); // code: None
        bytes.extend_from_slice(&2u32.to_le_bytes()); // data: 2 entries
        for key in [b'b', b'a'] {
            bytes.extend_from_slice(&1u32.to_le_bytes()); // key len
            bytes.push(key);
            bytes.extend_from_slice(&0u32.to_le_bytes()); // value len 0
        }
        bytes.extend_from_slice(&0u32.to_le_bytes()); // access_keys: empty

        // borsh's `de_strict_order` is off in this workspace, so the reader silently
        // re-sorts the keys; the re-serialize check in `from_raw` is what rejects this.
        let err = UniversalStateInit::from_raw(&RawStateInit(bytes)).unwrap_err();
        assert!(matches!(err, ParseRawStateInitError::NonCanonical), "got {err:?}");

        // The canonical ordering of the same map decodes cleanly.
        let mut ok = vec![0u8, 0u8];
        ok.extend_from_slice(&2u32.to_le_bytes());
        for key in [b'a', b'b'] {
            ok.extend_from_slice(&1u32.to_le_bytes());
            ok.push(key);
            ok.extend_from_slice(&0u32.to_le_bytes());
        }
        ok.extend_from_slice(&0u32.to_le_bytes());
        assert!(UniversalStateInit::from_raw(&RawStateInit(ok)).is_ok());
    }

    #[test]
    fn from_raw_rejects_unsorted_access_keys() {
        // Two access keys serialized in reverse `Ord` order. borsh re-sorts the set
        // on read, so only the re-serialize check catches it.
        let lo = borsh::to_vec(&ed_handle(1)).unwrap();
        let hi = borsh::to_vec(&ed_handle(2)).unwrap();
        let mut bytes = vec![0u8, 0u8]; // V1, code: None
        bytes.extend_from_slice(&0u32.to_le_bytes()); // data: empty
        bytes.extend_from_slice(&2u32.to_le_bytes()); // access_keys: 2 entries
        bytes.extend_from_slice(&hi); // reversed: handle(2) before handle(1)
        bytes.extend_from_slice(&lo);
        let err = UniversalStateInit::from_raw(&RawStateInit(bytes)).unwrap_err();
        assert!(matches!(err, ParseRawStateInitError::NonCanonical), "got {err:?}");
    }

    #[test]
    fn from_raw_rejects_duplicate_keys() {
        // A data map that declares 2 entries but repeats one key; borsh deduplicates on read.
        let mut bytes = vec![0u8, 0u8]; // V1, code: None
        bytes.extend_from_slice(&2u32.to_le_bytes()); // data: 2 entries
        for _ in 0..2 {
            bytes.extend_from_slice(&1u32.to_le_bytes());
            bytes.push(b'a');
            bytes.extend_from_slice(&0u32.to_le_bytes());
        }
        bytes.extend_from_slice(&0u32.to_le_bytes()); // access_keys: empty
        let err = UniversalStateInit::from_raw(&RawStateInit(bytes)).unwrap_err();
        assert!(matches!(err, ParseRawStateInitError::NonCanonical), "got {err:?}");
    }
}
