use std::{collections::HashMap, io, sync::Arc};

use borsh::BorshDeserialize;

use crate::runtime_utils::{get_runtime_and_trie, get_test_trie_viewer, TEST_SHARD_UID};
use near_primitives::{
    account::Account,
    hash::hash as sha256,
    hash::CryptoHash,
    serialize::to_base64,
    trie_key::trie_key_parsers,
    types::{AccountId, StateRoot},
    views::{StateItem, ViewApplyState},
};
use near_primitives::{
    test_utils::MockEpochInfoProvider,
    trie_key::TrieKey,
    types::{EpochId, StateChangeCause},
    version::PROTOCOL_VERSION,
};
use near_store::{set_account, NibbleSlice, RawTrieNode, RawTrieNodeWithSize};
use node_runtime::state_viewer::errors;
use node_runtime::state_viewer::*;
use testlib::runtime_utils::alice_account;

struct ProofVerifier {
    nodes: HashMap<CryptoHash, RawTrieNodeWithSize>,
}

impl ProofVerifier {
    fn new(proof: Vec<Arc<[u8]>>) -> Result<Self, io::Error> {
        let nodes = proof
            .into_iter()
            .map(|bytes| {
                let hash = CryptoHash::hash_bytes(&bytes);
                let node = RawTrieNodeWithSize::try_from_slice(&bytes)?;
                Ok((hash, node))
            })
            .collect::<Result<HashMap<_, _>, io::Error>>()?;
        Ok(Self { nodes })
    }

    fn verify(
        &self,
        state_root: &StateRoot,
        account_id: &AccountId,
        key: &[u8],
        expected: Option<&[u8]>,
    ) -> bool {
        let query = trie_key_parsers::get_raw_prefix_for_contract_data(account_id, key);
        let mut key = NibbleSlice::new(&query);

        let mut expected_hash = state_root;
        while let Some(node) = self.nodes.get(expected_hash) {
            match &node.node {
                RawTrieNode::Leaf(node_key, value) => {
                    let nib = &NibbleSlice::from_encoded(&node_key).0;
                    return if &key != nib {
                        expected.is_none()
                    } else {
                        expected.map_or(false, |expected| value == expected)
                    };
                }
                RawTrieNode::Extension(node_key, child_hash) => {
                    expected_hash = child_hash;

                    // To avoid unnecessary copy
                    let nib = NibbleSlice::from_encoded(&node_key).0;
                    if !key.starts_with(&nib) {
                        return expected.is_none();
                    }
                    key = key.mid(nib.len());
                }
                RawTrieNode::BranchNoValue(children) => {
                    if key.is_empty() {
                        return expected.is_none();
                    }
                    match children[key.at(0)] {
                        Some(ref child_hash) => {
                            key = key.mid(1);
                            expected_hash = child_hash;
                        }
                        None => return expected.is_none(),
                    }
                }
                RawTrieNode::BranchWithValue(value, children) => {
                    if key.is_empty() {
                        return expected.map_or(false, |exp| value == exp);
                    }
                    match children[key.at(0)] {
                        Some(ref child_hash) => {
                            key = key.mid(1);
                            expected_hash = child_hash;
                        }
                        None => return expected.is_none(),
                    }
                }
            }
        }
        false
    }
}

#[test]
fn test_view_call() {
    let (viewer, root) = get_test_trie_viewer();

    let mut logs = vec![];
    let view_state = ViewApplyState {
        block_height: 1,
        prev_block_hash: CryptoHash::default(),
        block_hash: CryptoHash::default(),
        epoch_id: EpochId::default(),
        epoch_height: 0,
        block_timestamp: 1,
        current_protocol_version: PROTOCOL_VERSION,
        cache: None,
    };
    let result = viewer.call_function(
        root,
        view_state,
        &"test.contract".parse().unwrap(),
        "run_test",
        &[],
        &mut logs,
        &MockEpochInfoProvider::default(),
    );

    assert_eq!(result.unwrap(), (10i32).to_le_bytes());
}

#[test]
fn test_view_call_try_changing_storage() {
    let (viewer, root) = get_test_trie_viewer();

    let mut logs = vec![];
    let view_state = ViewApplyState {
        block_height: 1,
        prev_block_hash: CryptoHash::default(),
        block_hash: CryptoHash::default(),
        epoch_id: EpochId::default(),
        epoch_height: 0,
        block_timestamp: 1,
        current_protocol_version: PROTOCOL_VERSION,
        cache: None,
    };
    let result = viewer.call_function(
        root,
        view_state,
        &"test.contract".parse().unwrap(),
        "run_test_with_storage_change",
        &[],
        &mut logs,
        &MockEpochInfoProvider::default(),
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains(r#"ProhibitedInView { method_name: "storage_write" }"#),
        "Got different error that doesn't match: {}",
        err
    );
}

#[test]
fn test_view_call_with_args() {
    let (viewer, root) = get_test_trie_viewer();
    let args: Vec<_> = [1u64, 2u64].iter().flat_map(|x| (*x).to_le_bytes().to_vec()).collect();
    let mut logs = vec![];
    let view_state = ViewApplyState {
        block_height: 1,
        prev_block_hash: CryptoHash::default(),
        block_hash: CryptoHash::default(),
        epoch_id: EpochId::default(),
        epoch_height: 0,
        block_timestamp: 1,
        current_protocol_version: PROTOCOL_VERSION,
        cache: None,
    };
    let view_call_result = viewer.call_function(
        root,
        view_state,
        &"test.contract".parse().unwrap(),
        "sum_with_input",
        &args,
        &mut logs,
        &MockEpochInfoProvider::default(),
    );
    assert_eq!(view_call_result.unwrap(), 3u64.to_le_bytes().to_vec());
}

fn assert_view_state(
    trie_viewer: &TrieViewer,
    state_update: &near_store::TrieUpdate,
    prefix: &[u8],
    want_values: &[(&[u8], &[u8])],
    want_proof: &[&'static str],
) -> ProofVerifier {
    let alice = alice_account();
    let alina = "alina".parse().unwrap();

    let values = want_values
        .iter()
        .map(|(key, value)| StateItem { key: key.to_vec().into(), value: value.to_vec().into() })
        .collect::<Vec<_>>();

    let view_state =
        |include_proof| trie_viewer.view_state(&state_update, &alice, prefix, include_proof);

    // Test without proof
    let result = view_state(false).unwrap();
    assert_eq!(values, result.values);
    assert_eq!(Vec::<Arc<[u8]>>::new(), result.proof);

    // Test with proof included
    let result = view_state(true).unwrap();
    assert_eq!(values, result.values);
    let got = result.proof.iter().map(|bytes| to_base64(bytes)).collect::<Vec<_>>();
    let got = got.iter().map(String::as_str).collect::<Vec<_>>();
    // The proof isn’t deterministic because the state contains test contracts
    // which aren’t built hermetically.  Fortunately, only the first two items
    // in the proof are affected.  First one is the state root which is an
    // Extension("0x0", child_hash) node and the second one is child hash
    // pointing to a Branch node which splits into four: 0x0 (accounts), 0x1
    // (contract code; that’s what’s nondeterministic), 0x2 (access keys) and
    // 0x9 (contract data; that’s what we care about).
    assert_eq!(&want_proof[..], &got[2..]);

    // Verify proofs for all the expected values.
    let proof_verifier = ProofVerifier::new(result.proof).unwrap();
    let root = state_update.get_root();
    for (key, value) in want_values {
        // Proof for known (key, value) should succeed.
        assert!(
            proof_verifier.verify(root, &alice, key, Some(value)),
            "key: alice / {key:x?}; value: {value:x?}"
        );
        // The key exist, so proof for non-existence should fail.
        assert!(
            !proof_verifier.verify(root, &alice, key, None),
            "key: alice / {key:x?}; value: None"
        );
        // Proof for different value should fail.
        assert!(
            !proof_verifier.verify(root, &alice, key, Some(b"bogus")),
            "key: alice / {key:x?}; value: None"
        );
        // Proofs for different account should fail.
        assert!(
            !proof_verifier.verify(root, &alina, key, Some(value)),
            "key: alice / {key:x?}; value: {value:x?}"
        );
        assert!(
            !proof_verifier.verify(root, &alina, key, None),
            "key: alice / {key:x?}; value: {value:x?}"
        );
    }

    proof_verifier
}

#[test]
fn test_view_state() {
    // in order to ensure determinism under all conditions (compiler, build output, etc)
    // avoid deploying a test contract. See issue #7238
    let (_, tries, root) = get_runtime_and_trie();
    let shard_uid = TEST_SHARD_UID;
    let mut state_update = tries.new_trie_update(shard_uid, root);
    state_update.set(
        TrieKey::ContractData { account_id: alice_account(), key: b"test123".to_vec() },
        b"123".to_vec(),
    );
    state_update.set(
        TrieKey::ContractData { account_id: alice_account(), key: b"test321".to_vec() },
        b"321".to_vec(),
    );
    state_update.set(
        TrieKey::ContractData { account_id: "alina".parse().unwrap(), key: b"qqq".to_vec() },
        b"321".to_vec(),
    );
    state_update.set(
        TrieKey::ContractData { account_id: "alex".parse().unwrap(), key: b"qqq".to_vec() },
        b"321".to_vec(),
    );
    state_update.commit(StateChangeCause::InitialState);
    let trie_changes = state_update.finalize().unwrap().1;
    let mut db_changes = tries.store_update();
    let new_root = tries.apply_all(&trie_changes, shard_uid, &mut db_changes);
    db_changes.commit().unwrap();

    let state_update = tries.new_trie_update(shard_uid, new_root);
    let trie_viewer = TrieViewer::default();

    let proof = [
        "AwMAAAAWFsbwm2TFX4GHLT5G1LSpF8UkG7zQV1ohXBMR/OQcUAKZ3gwDAAAAAAAA",
        "ASAC7S1KwgLNl0HPdSo8soL8sGOmPhL7O0xTSR8sDDR5pZrzu0ty3UPYJ5UKrFGKxXoyyyNG75AF9hnJHO3xxFkf5NQCAAAAAAAA",
        "AwEAAAAW607KPj2q3O8dF6XkfALiIrd9mqGir2UlYIcZuLNksTsvAgAAAAAAAA==",
        "AQhAP4sMdbiWZPtV6jz8hYKzRFSgwaSlQKiGsQXogAmMcrLOl+SJfiCOXMTEZ2a1ebmQOEGkRYa30FaIlB46sLI2IPsBAAAAAAAA",
        "AwwAAAAWUubmVhcix0ZXN0PKtrEndk0LxM+qpzp0PVtjf+xlrzz4TT0qA+hTtm6BLlYBAAAAAAAA",
        "AQoAVWCdny7wv/M1LvZASC3Fw0D/NNhI1NYwch9Ux+KZ2qRdQXPC1rNsCGRJ7nd66SfcNmRUVVvQY6EYCbsIiugO6gwBAAAAAAAA",
        "AAMAAAAgMjMDAAAApmWkWSBCL51Bfkhn79xPuKBKHz//H6B+mY6G9/eieuNtAAAAAAAAAA==",
        "AAMAAAAgMjEDAAAAjSPPbIboNKeqbt7VTCbOK7LnSQNTjGG91dIZeZerL3JtAAAAAAAAAA==",
    ];
    let values = [(&b"test123"[..], &b"123"[..]), (&b"test321"[..], &b"321"[..])];
    assert_view_state(&trie_viewer, &state_update, b"", &values, &proof);
    assert_view_state(&trie_viewer, &state_update, b"test", &values, &proof);

    assert_view_state(&trie_viewer, &state_update, b"xyz", &[], &[
        "AwMAAAAWFsbwm2TFX4GHLT5G1LSpF8UkG7zQV1ohXBMR/OQcUAKZ3gwDAAAAAAAA",
        "ASAC7S1KwgLNl0HPdSo8soL8sGOmPhL7O0xTSR8sDDR5pZrzu0ty3UPYJ5UKrFGKxXoyyyNG75AF9hnJHO3xxFkf5NQCAAAAAAAA",
        "AwEAAAAW607KPj2q3O8dF6XkfALiIrd9mqGir2UlYIcZuLNksTsvAgAAAAAAAA==",
        "AQhAP4sMdbiWZPtV6jz8hYKzRFSgwaSlQKiGsQXogAmMcrLOl+SJfiCOXMTEZ2a1ebmQOEGkRYa30FaIlB46sLI2IPsBAAAAAAAA",
        "AwwAAAAWUubmVhcix0ZXN0PKtrEndk0LxM+qpzp0PVtjf+xlrzz4TT0qA+hTtm6BLlYBAAAAAAAA",
    ][..]);

    let proof_verifier = assert_view_state(
        &trie_viewer,
        &state_update,
        b"test123",
        &[(&b"test123"[..], &b"123"[..])],
        &[
            "AwMAAAAWFsbwm2TFX4GHLT5G1LSpF8UkG7zQV1ohXBMR/OQcUAKZ3gwDAAAAAAAA",
            "ASAC7S1KwgLNl0HPdSo8soL8sGOmPhL7O0xTSR8sDDR5pZrzu0ty3UPYJ5UKrFGKxXoyyyNG75AF9hnJHO3xxFkf5NQCAAAAAAAA",
            "AwEAAAAW607KPj2q3O8dF6XkfALiIrd9mqGir2UlYIcZuLNksTsvAgAAAAAAAA==",
            "AQhAP4sMdbiWZPtV6jz8hYKzRFSgwaSlQKiGsQXogAmMcrLOl+SJfiCOXMTEZ2a1ebmQOEGkRYa30FaIlB46sLI2IPsBAAAAAAAA",
            "AwwAAAAWUubmVhcix0ZXN0PKtrEndk0LxM+qpzp0PVtjf+xlrzz4TT0qA+hTtm6BLlYBAAAAAAAA",
            "AQoAVWCdny7wv/M1LvZASC3Fw0D/NNhI1NYwch9Ux+KZ2qRdQXPC1rNsCGRJ7nd66SfcNmRUVVvQY6EYCbsIiugO6gwBAAAAAAAA",
            "AAMAAAAgMjMDAAAApmWkWSBCL51Bfkhn79xPuKBKHz//H6B+mY6G9/eieuNtAAAAAAAAAA==",
        ]
    );

    let root = state_update.get_root();
    let account = alice_account();
    for (want, key, value) in [
        (true, b"test123".as_ref(), Some(b"123".as_ref())),
        (false, b"test123".as_ref(), Some(b"321".as_ref())),
        (false, b"test123".as_ref(), Some(b"1234".as_ref())),
        (false, b"test123".as_ref(), None),
        // Shorter key:
        (false, b"test12".as_ref(), Some(b"123".as_ref())),
        (true, b"test12", None),
        // Longer key:
        (false, b"test1234", Some(b"123")),
        (true, b"test1234", None),
    ] {
        let got = proof_verifier.verify(&root, &account, key, value);
        assert_eq!(
            want,
            got,
            "key: {:?}; value: {:?}",
            std::str::from_utf8(key).unwrap(),
            value.map(|value| std::str::from_utf8(value).unwrap())
        );
    }
}

#[test]
fn test_view_state_too_large() {
    let (_, tries, root) = get_runtime_and_trie();
    let mut state_update = tries.new_trie_update(TEST_SHARD_UID, root);
    set_account(
        &mut state_update,
        alice_account(),
        &Account::new(0, 0, CryptoHash::default(), 50_001),
    );
    let trie_viewer = TrieViewer::new(Some(50_000), None);
    let result = trie_viewer.view_state(&state_update, &alice_account(), b"", false);
    assert!(matches!(result, Err(errors::ViewStateError::AccountStateTooLarge { .. })));
}

#[test]
fn test_view_state_with_large_contract() {
    let (_, tries, root) = get_runtime_and_trie();
    let mut state_update = tries.new_trie_update(TEST_SHARD_UID, root);
    let contract_code = [0; Account::MAX_ACCOUNT_DELETION_STORAGE_USAGE as usize].to_vec();
    set_account(
        &mut state_update,
        alice_account(),
        &Account::new(0, 0, sha256(&contract_code), 50_001),
    );
    state_update.set(TrieKey::ContractCode { account_id: alice_account() }, contract_code);
    let trie_viewer = TrieViewer::new(Some(50_000), None);
    let result = trie_viewer.view_state(&state_update, &alice_account(), b"", false);
    assert!(result.is_ok());
}

#[test]
fn test_log_when_panic() {
    let (viewer, root) = get_test_trie_viewer();
    let view_state = ViewApplyState {
        block_height: 1,
        prev_block_hash: CryptoHash::default(),
        block_hash: CryptoHash::default(),
        epoch_id: EpochId::default(),
        epoch_height: 0,
        block_timestamp: 1,
        current_protocol_version: PROTOCOL_VERSION,
        cache: None,
    };
    let mut logs = vec![];
    viewer
        .call_function(
            root,
            view_state,
            &"test.contract".parse().unwrap(),
            "panic_after_logging",
            &[],
            &mut logs,
            &MockEpochInfoProvider::default(),
        )
        .unwrap_err();

    assert_eq!(logs, vec!["hello".to_string()]);
}
