//! Known-answer vectors proving that deterministic (NEP-616 `0s`) contracts
//! deployed today re-derive as universal (`0u`) accounts.
//!
//! Each vector:
//!
//! 1. Reads a live account's storage.
//! 2. Rebuilds its [`DeterministicAccountStateInit`] and checks it re-derives the
//!    id that account actually has, so the fixture is the deployed state init.
//! 3. Re-expresses the same `code` and `data` as a
//!    [`UniversalStateInitV1`](crate::universal_state_init::UniversalStateInitV1)
//!    and records the `0u` id.
//!
//! `code` and `data` carry over byte for byte, with nothing left over. The one
//! field the universal form adds, `access_keys`, stays empty: these contracts
//! check their key themselves, so it lives in `data`. That is the convention.
//! Native keys go in `access_keys`, contract credentials stay in `data`.
//!
//! `test_universal_state_init_wallet_contract_vector` in the test-loop tests
//! reproduces the recorded ids on a running chain.

// cspell:words subwallet reimplementation
// cspell:ignore scdb cfeed

use crate::universal_state_init::{UniversalStateInit, UniversalStateInitV1};
use near_primitives_core::deterministic_account_id::{
    DeterministicAccountStateInit, DeterministicAccountStateInitV1,
};
use near_primitives_core::global_contract::GlobalContractIdentifier;
use near_primitives_core::types::AccountId;
use std::collections::{BTreeMap, BTreeSet};

/// One re-derivation vector: a deployed `0s` state init and its `0u` re-expression.
pub struct ReDerivationVector {
    /// The state init the deployed account's address was derived from.
    pub deterministic: DeterministicAccountStateInit,
    /// The same code and data as a universal state init.
    pub universal: UniversalStateInit,
    /// The `0s` account id the deployment actually has on chain.
    pub deterministic_account_id: AccountId,
    /// The `0u` account id the re-expression derives to.
    pub universal_account_id: AccountId,
}

/// The wallet contract from [near/intents](https://github.com/near/intents),
/// `contracts/wallet`, ed25519 variant, taken from a live mainnet instance. The
/// state layout below was read at `32a7836` (2026-08-19).
///
/// # Provenance
///
/// Account [`0scdb6cfeed476fc878af9d3246768cbe803714c87`][account], read at block
/// 214087065:
///
/// ```text
/// $ curl -s -X POST https://rpc.mainnet.near.org -H 'Content-Type: application/json' -d '{
///     "jsonrpc": "2.0", "id": 1, "method": "query",
///     "params": {
///       "request_type": "view_state", "finality": "final",
///       "account_id": "0scdb6cfeed476fc878af9d3246768cbe803714c87",
///       "prefix_base64": ""
///     }}'
/// ```
///
/// `view_account` on the same id reports
/// `global_contract_account_id: 0sb0d7ef4f935c6ef78e08ad03569767aaec4223a3`, which
/// is the `code` field below: the wallet code is deployed by account id, through
/// the project's Global Deployer, so that it stays upgradable in place.
///
/// The live storage value is 69 bytes and differs from the state the address was
/// derived from only in the two nonce fields the contract mutates as it is used
/// (`last_cleaned_at`, and the current nonce bitmap). `Nonces::new` starts both
/// at zero, so zeroing them recovers the initial state, which the id assertion in
/// [`wallet_contract`] confirms.
///
/// [account]: https://nearblocks.io/address/0scdb6cfeed476fc878af9d3246768cbe803714c87
pub mod wallet_contract {
    /// Global contract holding the ed25519 wallet code, addressed by account id.
    pub const GLOBAL_CONTRACT: &str = "0sb0d7ef4f935c6ef78e08ad03569767aaec4223a3";

    /// Storage key the wallet keeps its state under (`STATE_KEY` in the contract,
    /// which is the empty key).
    pub const STATE_KEY: &[u8] = b"";

    /// Borsh of the wallet's `State<Ed25519PublicKey>` as its own `as_storage()`
    /// writes it, with the nonce fields at their initial values.
    ///
    /// The public key sits in `data` because the wallet verifies signatures against
    /// it itself. It is not a protocol access key, which is why the universal form
    /// of this vector has no `access_keys`.
    pub const STATE: &str = concat!(
        "01",                                                               // signature_enabled
        "00000000",                                                         // subwallet_id: 0
        "8565df94b8caab08f28cdd2ee014b800915741d4694fa840e50cca02ae5c6466", // public_key
        "100e0000",                                                         // timeout: 3600s, u32
        "0000000000000000", // last_cleaned_at: unix epoch, u64 nanoseconds
        "00000000",         // nonces.old: empty bitmap
        "00000000",         // nonces.current: empty bitmap
        "00000000",         // extensions: none enabled
    );

    /// The account id the deployment has on mainnet.
    pub const DETERMINISTIC_ACCOUNT_ID: &str = "0scdb6cfeed476fc878af9d3246768cbe803714c87";

    /// Canonical borsh of the universal re-expression, which is what the `0u` id
    /// hashes. Published so a reimplementation can check its own derivation
    /// without rebuilding the struct.
    pub const UNIVERSAL_STATE_INIT: &str = concat!(
        "00",       // UniversalStateInit::V1
        "01",       // code: Some
        "01",       // GlobalContractIdentifier::AccountId
        "2a000000", // account id: 42 bytes
        // `GLOBAL_CONTRACT`
        "307362306437656634663933356336656637386530386164303335363937363761616563343232336133",
        "01000000", // data: 1 entry
        "00000000", // key: empty
        "3d000000", // value: 61 bytes,
        // `STATE`
        "01000000008565df94b8caab08f28cdd2ee014b800915741d4694fa840e50cca02",
        "ae5c6466100e00000000000000000000000000000000000000000000",
        "00000000", // access_keys: none
    );

    /// The account id the same state init derives to under the `0u` scheme.
    // cspell:disable-next-line
    pub const UNIVERSAL_ACCOUNT_ID: &str = "0u4bfkw2qvgfzbf7zzkxykcppqymn0p2hbayjee3ygzrbhmmtyejx0";
}

/// The [`wallet_contract`] vector, built into both of its forms.
pub fn wallet_contract() -> ReDerivationVector {
    let code = GlobalContractIdentifier::AccountId(
        wallet_contract::GLOBAL_CONTRACT.parse().expect("valid account id"),
    );
    let data = BTreeMap::from([(
        wallet_contract::STATE_KEY.to_vec(),
        hex::decode(wallet_contract::STATE).expect("valid hex"),
    )]);

    ReDerivationVector {
        deterministic: DeterministicAccountStateInit::V1(DeterministicAccountStateInitV1 {
            code: code.clone(),
            data: data.clone(),
        }),
        universal: UniversalStateInit::V1(UniversalStateInitV1 {
            code: Some(code),
            data,
            access_keys: BTreeSet::new(),
        }),
        deterministic_account_id: wallet_contract::DETERMINISTIC_ACCOUNT_ID
            .parse()
            .expect("valid account id"),
        universal_account_id: wallet_contract::UNIVERSAL_ACCOUNT_ID
            .parse()
            .expect("valid account id"),
    }
}

/// Every vector in this module.
pub fn all() -> Vec<ReDerivationVector> {
    vec![wallet_contract()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::{derive_near_deterministic_account_id, derive_universal_account_id};

    /// The fixture is the deployed state init, not a reconstruction that merely
    /// looks like one: it re-derives the id the account actually has on chain.
    #[test]
    fn vectors_reproduce_the_deployed_account_id() {
        for vector in all() {
            assert_eq!(
                derive_near_deterministic_account_id(&vector.deterministic),
                vector.deterministic_account_id,
            );
        }
    }

    /// The re-expression is lossless in both directions: `code` and `data` carry
    /// over unchanged, and the one field the universal form adds is unused.
    #[test]
    fn vectors_re_express_one_to_one() {
        for vector in all() {
            assert_eq!(vector.universal.code(), Some(vector.deterministic.code()));
            assert_eq!(vector.universal.data(), vector.deterministic.data());
            assert!(
                vector.universal.access_keys().is_empty(),
                "these contracts hold their credential in `data`, not as a protocol key",
            );
        }
    }

    /// The `0u` ids the vectors publish. Keep stable: they seed the NEP, and the
    /// test-loop tests pin the on-chain host function against them.
    #[test]
    fn vectors_derive_the_recorded_universal_account_id() {
        for vector in all() {
            assert_eq!(
                derive_universal_account_id(&vector.universal.to_raw()),
                vector.universal_account_id,
            );
        }
    }

    /// The published bytes are the ones the id hashes, so a reimplementation can
    /// start from the hex rather than from our struct.
    #[test]
    fn published_bytes_match_the_typed_form() {
        assert_eq!(
            hex::encode(wallet_contract().universal.to_raw().0),
            wallet_contract::UNIVERSAL_STATE_INIT,
        );
    }

    /// The two schemes address the same contract differently, which is the point
    /// of keeping them disjoint.
    #[test]
    fn the_two_schemes_disagree() {
        for vector in all() {
            assert_ne!(vector.deterministic_account_id, vector.universal_account_id);
        }
    }
}
