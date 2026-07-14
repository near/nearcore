use crate::utils::runtime_utils::{TEST_SHARD_UID, get_runtime_and_trie, get_test_trie_viewer};
use borsh::BorshDeserialize;
use near_chain_configs::default_view_access_keys_limit;
use near_parameters::RuntimeConfigStore;
use near_primitives::{
    account::{Account, AccountContract},
    hash::{CryptoHash, hash as sha256},
    serialize::to_base64,
    trie_key::trie_key_parsers,
    types::{AccountId, Balance, StateRoot, StoreKey},
    views::StateItem,
};
use near_primitives::{
    test_utils::MockEpochInfoProvider,
    trie_key::TrieKey,
    types::{EpochId, StateChangeCause},
    version::PROTOCOL_VERSION,
};
use near_store::{
    NibbleSlice, RawTrieNode, RawTrieNodeWithSize, ShardTries, ShardUId, TrieUpdate, set_account,
};
use node_runtime::state_viewer::errors;
use node_runtime::state_viewer::*;
use std::num::NonZeroU32;
use std::{collections::HashMap, io, sync::Arc};
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
                        expected.is_some_and(|expected| value == expected)
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
                        return expected.is_some_and(|exp| value == exp);
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
        shard_id: ShardUId::single_shard().shard_id(),
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
        shard_id: ShardUId::single_shard().shard_id(),
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
        shard_id: ShardUId::single_shard().shard_id(),
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
    // cspell:ignore alina
    let alice = alice_account();
    let alina = "alina".parse().unwrap();

    let values = want_values
        .iter()
        .map(|(key, value)| StateItem { key: key.to_vec().into(), value: value.to_vec().into() })
        .collect::<Vec<_>>();

    let view_state = |include_proof| {
        trie_viewer.view_state(&state_update, &alice, prefix, None, None, include_proof)
    };

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
    let trie_changes = state_update.finalize().unwrap().trie_changes;
    let mut db_changes = tries.store_update();
    let new_root = tries.apply_all(&trie_changes, shard_uid, &mut db_changes);
    db_changes.commit();

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

    assert_view_state(
        &trie_viewer,
        &state_update,
        b"xyz",
        &[],
        &[
            "AwMAAAAWFsbwm2TFX4GHLT5G1LSpF8UkG7zQV1ohXBMR/OQcUAKZ3gwDAAAAAAAA",
            "ASAC7S1KwgLNl0HPdSo8soL8sGOmPhL7O0xTSR8sDDR5pZrzu0ty3UPYJ5UKrFGKxXoyyyNG75AF9hnJHO3xxFkf5NQCAAAAAAAA",
            "AwEAAAAW607KPj2q3O8dF6XkfALiIrd9mqGir2UlYIcZuLNksTsvAgAAAAAAAA==",
            "AQhAP4sMdbiWZPtV6jz8hYKzRFSgwaSlQKiGsQXogAmMcrLOl+SJfiCOXMTEZ2a1ebmQOEGkRYa30FaIlB46sLI2IPsBAAAAAAAA",
            "AwwAAAAWUubmVhcix0ZXN0PKtrEndk0LxM+qpzp0PVtjf+xlrzz4TT0qA+hTtm6BLlYBAAAAAAAA",
        ][..],
    );

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
        ],
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
        &Account::new(Balance::ZERO, Balance::ZERO, AccountContract::None, 50_001),
    );
    let trie_viewer = TrieViewer::new(
        RuntimeConfigStore::new(None),
        Some(50_000),
        default_view_access_keys_limit(),
        None,
    );
    let result = trie_viewer.view_state(&state_update, &alice_account(), b"", None, None, false);
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
        &Account::new(
            Balance::ZERO,
            Balance::ZERO,
            AccountContract::from_local_code_hash(sha256(&contract_code)),
            50_001,
        ),
    );
    state_update.set(TrieKey::ContractCode { account_id: alice_account() }, contract_code);
    let trie_viewer = TrieViewer::new(
        RuntimeConfigStore::new(None),
        Some(50_000),
        default_view_access_keys_limit(),
        None,
    );
    let result = trie_viewer.view_state(&state_update, &alice_account(), b"", None, None, false);
    assert!(result.is_ok());
}

#[test]
fn test_log_when_panic() {
    let (viewer, root) = get_test_trie_viewer();
    let view_state = ViewApplyState {
        block_height: 1,
        prev_block_hash: CryptoHash::default(),
        shard_id: ShardUId::single_shard().shard_id(),
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

fn nz(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

fn data_item(key: &[u8], value: &[u8]) -> StateItem {
    StateItem { key: key.to_vec().into(), value: value.to_vec().into() }
}

/// Commit `entries` (account, contract-data key, value) to disk and return a fresh view.
fn view_with_contract_data(entries: &[(AccountId, Vec<u8>, Vec<u8>)]) -> TrieUpdate {
    let (_, tries, root) = get_runtime_and_trie();
    let mut state_update = tries.new_trie_update(TEST_SHARD_UID, root);
    for (account_id, key, value) in entries {
        state_update.set(
            TrieKey::ContractData { account_id: account_id.clone(), key: key.clone() },
            value.clone(),
        );
    }
    commit_and_view(tries, state_update)
}

fn commit_and_view(tries: ShardTries, mut state_update: TrieUpdate) -> TrieUpdate {
    state_update.commit(StateChangeCause::InitialState);
    let trie_changes = state_update.finalize().unwrap().trie_changes;
    let mut db_changes = tries.store_update();
    let new_root = tries.apply_all(&trie_changes, TEST_SHARD_UID, &mut db_changes);
    db_changes.commit();
    tries.new_trie_update(TEST_SHARD_UID, new_root)
}

/// Walk every page, following `last_key`, and return the concatenated items.
fn collect_pages(
    viewer: &TrieViewer,
    state_update: &TrieUpdate,
    account_id: &AccountId,
    prefix: &[u8],
    limit: Option<NonZeroU32>,
) -> Vec<StateItem> {
    let mut all = Vec::new();
    let mut after_key: Option<StoreKey> = None;
    for _ in 0..10_000 {
        let result = viewer
            .view_state(
                state_update,
                account_id,
                prefix,
                after_key.as_ref().map(|k| k.as_slice()),
                limit,
                false,
            )
            .unwrap();
        all.extend(result.values);
        match result.last_key {
            Some(key) => after_key = Some(key),
            None => return all,
        }
    }
    panic!("pagination did not terminate");
}

/// Ten contract-data entries `key00..key09` with values `val0..val9`, all owned by alice.
fn ten_entries() -> Vec<(AccountId, Vec<u8>, Vec<u8>)> {
    (0..10)
        .map(|i| {
            (alice_account(), format!("key{i:02}").into_bytes(), format!("val{i}").into_bytes())
        })
        .collect()
}

#[test]
fn test_view_state_pagination_basic() {
    let state_update = view_with_contract_data(&ten_entries());
    let viewer = TrieViewer::default();
    let alice = alice_account();

    let page1 = viewer.view_state(&state_update, &alice, b"", None, Some(nz(3)), false).unwrap();
    assert_eq!(
        page1.values,
        vec![
            data_item(b"key00", b"val0"),
            data_item(b"key01", b"val1"),
            data_item(b"key02", b"val2"),
        ]
    );
    assert_eq!(page1.last_key, Some(b"key02".to_vec().into()));

    // Resuming from the cursor skips that key.
    let page2 =
        viewer.view_state(&state_update, &alice, b"", Some(b"key02"), Some(nz(3)), false).unwrap();
    assert_eq!(
        page2.values,
        vec![
            data_item(b"key03", b"val3"),
            data_item(b"key04", b"val4"),
            data_item(b"key05", b"val5"),
        ]
    );
    assert_eq!(page2.last_key, Some(b"key05".to_vec().into()));

    let all = collect_pages(&viewer, &state_update, &alice, b"", Some(nz(3)));
    let expected: Vec<_> =
        ten_entries().iter().map(|(_, key, value)| data_item(key, value)).collect();
    assert_eq!(all, expected);
}

#[test]
fn test_view_state_pagination_exact_boundary() {
    let state_update = view_with_contract_data(&ten_entries());
    let viewer = TrieViewer::default();
    let alice = alice_account();

    let page1 = viewer.view_state(&state_update, &alice, b"", None, Some(nz(5)), false).unwrap();
    assert_eq!(page1.values.len(), 5);
    assert_eq!(page1.last_key, Some(b"key04".to_vec().into()));

    // The final full page must not report a last_key when nothing follows.
    let page2 =
        viewer.view_state(&state_update, &alice, b"", Some(b"key04"), Some(nz(5)), false).unwrap();
    assert_eq!(page2.values.len(), 5);
    assert_eq!(page2.last_key, None);
}

#[test]
fn test_view_state_pagination_limit_exceeds_total() {
    let state_update = view_with_contract_data(&ten_entries());
    let viewer = TrieViewer::default();
    let alice = alice_account();

    for limit in [nz(10), nz(100), nz(u32::MAX)] {
        let result =
            viewer.view_state(&state_update, &alice, b"", None, Some(limit), false).unwrap();
        assert_eq!(result.values.len(), 10, "limit {limit}");
        assert_eq!(result.last_key, None, "limit {limit}");
    }
}

#[test]
fn test_view_state_pagination_after_key_without_limit() {
    let state_update = view_with_contract_data(&ten_entries());
    let viewer = TrieViewer::default();
    let alice = alice_account();

    let result =
        viewer.view_state(&state_update, &alice, b"", Some(b"key04"), None, false).unwrap();
    let expected: Vec<_> = (5..10)
        .map(|i| data_item(format!("key{i:02}").as_bytes(), format!("val{i}").as_bytes()))
        .collect();
    assert_eq!(result.values, expected);
    assert_eq!(result.last_key, None);
}

#[test]
fn test_view_state_pagination_after_key_past_end() {
    let state_update = view_with_contract_data(&ten_entries());
    let viewer = TrieViewer::default();
    let alice = alice_account();

    let result =
        viewer.view_state(&state_update, &alice, b"", Some(b"zzzz"), Some(nz(3)), false).unwrap();
    assert!(result.values.is_empty());
    assert_eq!(result.last_key, None);
}

#[test]
fn test_view_state_pagination_with_prefix() {
    let entries = vec![
        (alice_account(), b"aaa0".to_vec(), b"a0".to_vec()),
        (alice_account(), b"aaa1".to_vec(), b"a1".to_vec()),
        (alice_account(), b"bbb0".to_vec(), b"b0".to_vec()),
        (alice_account(), b"bbb1".to_vec(), b"b1".to_vec()),
    ];
    let state_update = view_with_contract_data(&entries);
    let viewer = TrieViewer::default();
    let alice = alice_account();

    let page1 = viewer.view_state(&state_update, &alice, b"aaa", None, Some(nz(1)), false).unwrap();
    assert_eq!(page1.values, vec![data_item(b"aaa0", b"a0")]);
    assert_eq!(page1.last_key, Some(b"aaa0".to_vec().into()));

    // The last in-prefix item reports no `last_key` even though `bbb*` rows follow it.
    let page2 = viewer
        .view_state(&state_update, &alice, b"aaa", Some(b"aaa0"), Some(nz(1)), false)
        .unwrap();
    assert_eq!(page2.values, vec![data_item(b"aaa1", b"a1")]);
    assert_eq!(page2.last_key, None);

    let all = collect_pages(&viewer, &state_update, &alice, b"aaa", Some(nz(1)));
    assert_eq!(all, vec![data_item(b"aaa0", b"a0"), data_item(b"aaa1", b"a1")]);
}

#[test]
fn test_view_state_pagination_empty() {
    let viewer = TrieViewer::default();
    let alice = alice_account();

    // Account with no contract data.
    let state_update = view_with_contract_data(&[]);
    let result = viewer.view_state(&state_update, &alice, b"", None, Some(nz(5)), false).unwrap();
    assert!(result.values.is_empty());
    assert_eq!(result.last_key, None);

    // Prefix that matches nothing.
    let state_update =
        view_with_contract_data(&[(alice_account(), b"key0".to_vec(), b"v".to_vec())]);
    let result =
        viewer.view_state(&state_update, &alice, b"nomatch", None, Some(nz(5)), false).unwrap();
    assert!(result.values.is_empty());
    assert_eq!(result.last_key, None);
}

#[test]
fn test_view_state_pagination_byte_cap() {
    let big = vec![b'x'; 20_000];
    let entries: Vec<_> =
        (0..5).map(|i| (alice_account(), format!("bk{i}").into_bytes(), big.clone())).collect();
    let state_update = view_with_contract_data(&entries);
    let viewer = TrieViewer::default();
    let alice = alice_account();

    // limit 100 keeps the item cap out of the way: the 50 KB byte cap ends the page,
    // since a 3rd 20 KB value crosses it.
    let page1 = viewer.view_state(&state_update, &alice, b"", None, Some(nz(100)), false).unwrap();
    assert_eq!(page1.values.len(), 3);
    assert_eq!(page1.last_key, Some(b"bk2".to_vec().into()));

    let page2 =
        viewer.view_state(&state_update, &alice, b"", Some(b"bk2"), Some(nz(100)), false).unwrap();
    assert_eq!(page2.values.len(), 2);
    assert_eq!(page2.last_key, None);

    let all = collect_pages(&viewer, &state_update, &alice, b"", Some(nz(100)));
    assert_eq!(all.len(), 5);
}

#[test]
fn test_view_state_pagination_account_isolation() {
    // cspell:ignore akey
    // alice and bob deliberately share identical contract-data keys.
    let bob: AccountId = "bob.near".parse().unwrap();
    let mut entries = Vec::new();
    for i in 0..5 {
        entries.push((
            alice_account(),
            format!("akey{i}").into_bytes(),
            format!("av{i}").into_bytes(),
        ));
    }
    for i in 0..5 {
        entries.push((bob.clone(), format!("akey{i}").into_bytes(), format!("bv{i}").into_bytes()));
    }
    let state_update = view_with_contract_data(&entries);
    let viewer = TrieViewer::default();

    let all = collect_pages(&viewer, &state_update, &alice_account(), b"", Some(nz(2)));
    let expected: Vec<_> = (0..5)
        .map(|i| data_item(format!("akey{i}").as_bytes(), format!("av{i}").as_bytes()))
        .collect();
    assert_eq!(all, expected);
}

#[test]
fn test_view_state_pagination_bypasses_size_limit() {
    let (_, tries, root) = get_runtime_and_trie();
    let mut state_update = tries.new_trie_update(TEST_SHARD_UID, root);
    set_account(
        &mut state_update,
        alice_account(),
        &Account::new(Balance::ZERO, Balance::ZERO, AccountContract::None, 50_001),
    );
    for i in 0..3 {
        state_update.set(
            TrieKey::ContractData {
                account_id: alice_account(),
                key: format!("k{i}").into_bytes(),
            },
            format!("v{i}").into_bytes(),
        );
    }
    let state_update = commit_and_view(tries, state_update);
    let viewer = TrieViewer::new(
        RuntimeConfigStore::new(None),
        Some(50_000),
        default_view_access_keys_limit(),
        None,
    );
    let alice = alice_account();

    let unpaginated = viewer.view_state(&state_update, &alice, b"", None, None, false);
    assert!(matches!(unpaginated, Err(errors::ViewStateError::AccountStateTooLarge { .. })));

    let paged = viewer.view_state(&state_update, &alice, b"", None, Some(nz(2)), false).unwrap();
    assert_eq!(paged.values, vec![data_item(b"k0", b"v0"), data_item(b"k1", b"v1")]);
    assert_eq!(paged.last_key, Some(b"k1".to_vec().into()));

    let resumed = viewer.view_state(&state_update, &alice, b"", Some(b"k1"), None, false).unwrap();
    assert_eq!(resumed.values, vec![data_item(b"k2", b"v2")]);
    assert_eq!(resumed.last_key, None);
}

#[test]
fn test_view_state_pagination_rejects_include_proof() {
    let state_update = view_with_contract_data(&ten_entries());
    let viewer = TrieViewer::default();
    let alice = alice_account();

    let cases: [(Option<&[u8]>, Option<NonZeroU32>); 3] =
        [(Some(b"key02"), None), (None, Some(nz(3))), (Some(b"key02"), Some(nz(3)))];
    for (after_key, limit) in cases {
        let result = viewer.view_state(&state_update, &alice, b"", after_key, limit, true);
        assert!(matches!(result, Err(errors::ViewStateError::ProofUnsupportedWithPagination)));
    }
}

#[test]
fn test_view_state_pagination_rejects_after_key_outside_prefix() {
    let state_update =
        view_with_contract_data(&[(alice_account(), b"aaa0".to_vec(), b"v".to_vec())]);
    let viewer = TrieViewer::default();
    let alice = alice_account();

    for after_key in [b"aa".as_slice(), b"bbb".as_slice()] {
        let result = viewer.view_state(&state_update, &alice, b"aaa", Some(after_key), None, false);
        assert!(matches!(result, Err(errors::ViewStateError::AfterKeyOutsidePrefix)));
    }
}
