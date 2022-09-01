use std::{collections::HashMap, io, sync::Arc};

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
use testlib::runtime_utils::{alice_account, encode_int};

struct ProofVerifier {
    nodes: HashMap<CryptoHash, RawTrieNodeWithSize>,
}

impl ProofVerifier {
    fn new(proof: Vec<Arc<[u8]>>) -> Result<Self, io::Error> {
        let nodes = proof
            .into_iter()
            .map(|bytes| {
                let hash = CryptoHash::hash_bytes(&bytes);
                let node = RawTrieNodeWithSize::decode(&bytes)?;
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
                RawTrieNode::Leaf(node_key, value_length, value_hash) => {
                    let nib = &NibbleSlice::from_encoded(&node_key).0;
                    return if &key != nib {
                        return expected.is_none();
                    } else if let Some(value) = expected {
                        if *value_length as usize != value.len() {
                            return false;
                        }
                        CryptoHash::hash_bytes(value) == *value_hash
                    } else {
                        false
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
                RawTrieNode::Branch(children, value) => {
                    if key.is_empty() {
                        return *value
                            == expected.map(|value| {
                                (value.len().try_into().unwrap(), CryptoHash::hash_bytes(&value))
                            });
                    }
                    let index = key.at(0);
                    match &children[index as usize] {
                        Some(child_hash) => {
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

    assert_eq!(result.unwrap(), encode_int(10));
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

#[track_caller]
fn assert_proof(proof: &[Arc<[u8]>], want: &[&'static str]) {
    let got = proof.iter().map(|bytes| to_base64(bytes)).collect::<Vec<_>>();
    let got = got.iter().map(String::as_str).collect::<Vec<_>>();
    assert_eq!(want, &got[..]);
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
    let trie_changes = state_update.finalize().unwrap().0;
    let (db_changes, new_root) = tries.apply_all(&trie_changes, shard_uid);
    db_changes.commit().unwrap();

    let state_update = tries.new_trie_update(shard_uid, new_root);
    let trie_viewer = TrieViewer::default();
    let result = trie_viewer.view_state(&state_update, &alice_account(), b"").unwrap();
    // there's a source of non determinism coming from the fact that
    // we’re adding a test contract to the state and that contract is not built hermetically
    // hence ignoring the first part of the proof
    assert_proof(&result.proof[2..], &[
        "AwEAAAAQjHWWT6rXAXqUm14fjfDxo3286ApntHMI1eK0aQAJZPfJewEAAAAAAA==",
        "AQcCSXBK8DHIYBF47dz6xB2iFKLLsPjAIAo9syJTBC0/Y1OjJNvT5izZukYCmtq/AyVTeyWFl1Ei6yFZBf5yIJ0i96eYRr8PVilJ81MgJKvV/R1SxQuTfwwmbZ6sN/TC2XfL1SCJ4WM1GZ0yMSaNpJOdsJH9kda203WM3Zh81gxz6rmVewEAAAAAAA==",
        "AwMAAAAWFsbwm2TFX4GHLT5G1LSpF8UkG7zQV1ohXBMR/OQcUAKZ3gwDAAAAAAAA",
        "ASAC7S1KwgLNl0HPdSo8soL8sGOmPhL7O0xTSR8sDDR5pZrzu0ty3UPYJ5UKrFGKxXoyyyNG75AF9hnJHO3xxFkf5NQCAAAAAAAA",
        "AwEAAAAW607KPj2q3O8dF6XkfALiIrd9mqGir2UlYIcZuLNksTsvAgAAAAAAAA==",
        "AQhAP4sMdbiWZPtV6jz8hYKzRFSgwaSlQKiGsQXogAmMcrLOl+SJfiCOXMTEZ2a1ebmQOEGkRYa30FaIlB46sLI2IPsBAAAAAAAA",
        "AwwAAAAWUubmVhcix0ZXN0PKtrEndk0LxM+qpzp0PVtjf+xlrzz4TT0qA+hTtm6BLlYBAAAAAAAA",
        "AQoAVWCdny7wv/M1LvZASC3Fw0D/NNhI1NYwch9Ux+KZ2qRdQXPC1rNsCGRJ7nd66SfcNmRUVVvQY6EYCbsIiugO6gwBAAAAAAAA",
        "AAMAAAAgMjMDAAAApmWkWSBCL51Bfkhn79xPuKBKHz//H6B+mY6G9/eieuNtAAAAAAAAAA==",
        "AAMAAAAgMjEDAAAAjSPPbIboNKeqbt7VTCbOK7LnSQNTjGG91dIZeZerL3JtAAAAAAAAAA==",
        "AAYAAAAgYSxxcXEDAAAAjSPPbIboNKeqbt7VTCbOK7LnSQNTjGG91dIZeZerL3JzAAAAAAAAAA==",
    ][2..]);
    assert_eq!(
        result.values,
        [
            StateItem { key: b"test123".to_vec(), value: b"123".to_vec(), proof: vec![] },
            StateItem { key: b"test321".to_vec(), value: b"321".to_vec(), proof: vec![] }
        ]
    );
    let result = trie_viewer.view_state(&state_update, &alice_account(), b"xyz").unwrap();
    assert_eq!(result.values, []);
    let result = trie_viewer.view_state(&state_update, &alice_account(), b"test123").unwrap();
    assert_eq!(
        result.values,
        [StateItem { key: b"test123".to_vec(), value: b"123".to_vec(), proof: vec![] }]
    );
    assert_proof(
        &result
            .proof[2..],
        &[
            "AwEAAAAQjHWWT6rXAXqUm14fjfDxo3286ApntHMI1eK0aQAJZPfJewEAAAAAAA==",
            "AQcCSXBK8DHIYBF47dz6xB2iFKLLsPjAIAo9syJTBC0/Y1OjJNvT5izZukYCmtq/AyVTeyWFl1Ei6yFZBf5yIJ0i96eYRr8PVilJ81MgJKvV/R1SxQuTfwwmbZ6sN/TC2XfL1SCJ4WM1GZ0yMSaNpJOdsJH9kda203WM3Zh81gxz6rmVewEAAAAAAA==",
            "AwMAAAAWFsbwm2TFX4GHLT5G1LSpF8UkG7zQV1ohXBMR/OQcUAKZ3gwDAAAAAAAA",
            "ASAC7S1KwgLNl0HPdSo8soL8sGOmPhL7O0xTSR8sDDR5pZrzu0ty3UPYJ5UKrFGKxXoyyyNG75AF9hnJHO3xxFkf5NQCAAAAAAAA",
            "AwEAAAAW607KPj2q3O8dF6XkfALiIrd9mqGir2UlYIcZuLNksTsvAgAAAAAAAA==",
            "AQhAP4sMdbiWZPtV6jz8hYKzRFSgwaSlQKiGsQXogAmMcrLOl+SJfiCOXMTEZ2a1ebmQOEGkRYa30FaIlB46sLI2IPsBAAAAAAAA",
            "AwwAAAAWUubmVhcix0ZXN0PKtrEndk0LxM+qpzp0PVtjf+xlrzz4TT0qA+hTtm6BLlYBAAAAAAAA",
            "AQoAVWCdny7wv/M1LvZASC3Fw0D/NNhI1NYwch9Ux+KZ2qRdQXPC1rNsCGRJ7nd66SfcNmRUVVvQY6EYCbsIiugO6gwBAAAAAAAA",
            "AAMAAAAgMjMDAAAApmWkWSBCL51Bfkhn79xPuKBKHz//H6B+mY6G9/eieuNtAAAAAAAAAA==",
            "AAMAAAAgMjEDAAAAjSPPbIboNKeqbt7VTCbOK7LnSQNTjGG91dIZeZerL3JtAAAAAAAAAA==",
        ][2..]
    );

    let root = state_update.get_root();
    let account = alice_account();
    let proof_verifier =
        ProofVerifier::new(result.proof).expect("could not create a ProofVerifier");
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
        let got = proof_verifier.verify(root, &account, key, value);
        assert_eq!(want, got, "key: {key:x?}; value: {value:x?}");
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
    let result = trie_viewer.view_state(&state_update, &alice_account(), b"");
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
    let result = trie_viewer.view_state(&state_update, &alice_account(), b"");
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
