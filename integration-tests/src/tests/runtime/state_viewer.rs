use std::str::FromStr;

use crate::runtime_utils::{get_runtime_and_trie, get_test_trie_viewer, TEST_SHARD_UID};
use near_primitives::{
    account::Account,
    hash::hash as sha256,
    hash::CryptoHash,
    state_proof::{TrieProofBranch, TrieProofExtension, TrieProofItem, TrieProofLeaf},
    views::{StateItem, ViewApplyState},
};
use near_primitives::{
    test_utils::MockEpochInfoProvider,
    trie_key::TrieKey,
    types::{EpochId, StateChangeCause},
    version::PROTOCOL_VERSION,
};
use near_store::set_account;
use node_runtime::state_viewer::errors;
use node_runtime::state_viewer::*;
use testlib::runtime_utils::{alice_account, encode_int};

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

#[test]
fn test_view_state() {
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
    assert_eq!(result.proof, Vec::<_>::new());
    assert_eq!(
        result.values,
        [
            StateItem { key: "dGVzdDEyMw==".to_string(), value: "MTIz".to_string(), proof: vec![] },
            StateItem { key: "dGVzdDMyMQ==".to_string(), value: "MzIx".to_string(), proof: vec![] }
        ]
    );
    let result = trie_viewer.view_state(&state_update, &alice_account(), b"xyz").unwrap();
    assert_eq!(result.values, []);
    let result = trie_viewer.view_state(&state_update, &alice_account(), b"test123").unwrap();
    assert_eq!(
        result.values,
        [StateItem { key: "dGVzdDEyMw==".to_string(), value: "MTIz".to_string(), proof: vec![] }]
    );
    assert_eq!(
        result.proof,
        vec![
            TrieProofItem::Extension(Box::new(TrieProofExtension {
                key: vec![16],
                child_hash: CryptoHash::from_str("FwUXNsdcWaTQdZHgmHDjXuCGPmS8f4veZEKSpnNwFL6e")
                    .unwrap(),
                memory_usage: 97430
            })),
            TrieProofItem::Branch(Box::new(TrieProofBranch {
                children: [
                    Some(
                        CryptoHash::from_str("8A6QyKBCCAAtVAeSJcgGtUaFQZCdpd4W8hG9kHNV67WH")
                            .unwrap()
                    ),
                    Some(
                        CryptoHash::from_str("Ci8TDPmfawsHAVC9tUCvRuJKUTAhPJGrpgGbxKNBCroY")
                            .unwrap()
                    ),
                    Some(
                        CryptoHash::from_str("CHDm3ySyAKvcb6nctEaGfqjY7EGLz9tXJZ28XEXGDi6N")
                            .unwrap()
                    ),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(
                        CryptoHash::from_str("FLxcnKrm7fPnZNdeVvybWtnQGBmyoUuXjhXmhq3NgNPa")
                            .unwrap()
                    ),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None
                ],
                value: None,
                memory_usage: 97378,
                index: 9
            })),
            TrieProofItem::Branch(Box::new(TrieProofBranch {
                children: [
                    Some(
                        CryptoHash::from_str("8A6QyKBCCAAtVAeSJcgGtUaFQZCdpd4W8hG9kHNV67WH")
                            .unwrap()
                    ),
                    Some(
                        CryptoHash::from_str("Ci8TDPmfawsHAVC9tUCvRuJKUTAhPJGrpgGbxKNBCroY")
                            .unwrap()
                    ),
                    Some(
                        CryptoHash::from_str("CHDm3ySyAKvcb6nctEaGfqjY7EGLz9tXJZ28XEXGDi6N")
                            .unwrap()
                    ),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None
                ],
                value: None,
                memory_usage: 97378,
                index: 9
            })),
            TrieProofItem::Extension(Box::new(TrieProofExtension {
                key: vec![22, 22, 198],
                child_hash: CryptoHash::from_str("HCEH9UoyeZUv5UDgu9QpFTxWoFGPMrS4VAV2TZHzSLjw")
                    .unwrap(),
                memory_usage: 780
            })),
            TrieProofItem::Branch(Box::new(TrieProofBranch {
                children: [
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(
                        CryptoHash::from_str("GxqgVRkFxzLvar8iHeBADqu2PpRiH3fYVdPGzw7zUdzR")
                            .unwrap()
                    ),
                    None,
                    None,
                    None,
                    Some(
                        CryptoHash::from_str("HQRiU7i61GFKWtorr8RzY1QyQVJqX5hrytYqwhiyHZHd")
                            .unwrap()
                    ),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None
                ],
                value: None,
                memory_usage: 724,
                index: 9
            })),
            TrieProofItem::Branch(Box::new(TrieProofBranch {
                children: [
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(
                        CryptoHash::from_str("GxqgVRkFxzLvar8iHeBADqu2PpRiH3fYVdPGzw7zUdzR")
                            .unwrap()
                    ),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None
                ],
                value: None,
                memory_usage: 724,
                index: 9
            })),
            TrieProofItem::Extension(Box::new(TrieProofExtension {
                key: vec![22],
                child_hash: CryptoHash::from_str("GqYVVbkA7nvAEuCsvKZdWQ7Ddc5e9bNsfVhTTusWyvup")
                    .unwrap(),
                memory_usage: 559
            })),
            TrieProofItem::Branch(Box::new(TrieProofBranch {
                children: [
                    None,
                    None,
                    None,
                    Some(
                        CryptoHash::from_str("5H3fJHA6jCZKnbFnkFDcuPDRbPdmiSgC4mR4jX4D5XD3")
                            .unwrap()
                    ),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(
                        CryptoHash::from_str("EuTKawKwL1A9pgVPxV1EHpzFYYBxjy9DP55VkcWX4GQP")
                            .unwrap()
                    ),
                    None
                ],
                value: None,
                memory_usage: 507,
                index: 3
            })),
            TrieProofItem::Branch(Box::new(TrieProofBranch {
                children: [
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(
                        CryptoHash::from_str("EuTKawKwL1A9pgVPxV1EHpzFYYBxjy9DP55VkcWX4GQP")
                            .unwrap()
                    ),
                    None
                ],
                value: None,
                memory_usage: 507,
                index: 3
            })),
            TrieProofItem::Extension(Box::new(TrieProofExtension {
                key: vec![22, 82, 230, 230, 86, 23, 34, 199, 70, 87, 55, 67],
                child_hash: CryptoHash::from_str("EeJvs6otTUKtsbpfcgm59ePFGw7QMB5G37hAVAS6n341")
                    .unwrap(),
                memory_usage: 342
            })),
            TrieProofItem::Branch(Box::new(TrieProofBranch {
                children: [
                    None,
                    Some(
                        CryptoHash::from_str("6kH6uEShmyoX7iGisy8nZxoYVNw6w8McqqDgXHct7J6X")
                            .unwrap()
                    ),
                    None,
                    Some(
                        CryptoHash::from_str("7H2oXQXYxnuB3Kq6djTnS7rkTdsgmLhByL3JFHLsrFQM")
                            .unwrap()
                    ),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None
                ],
                value: None,
                memory_usage: 268,
                index: 1
            })),
            TrieProofItem::Branch(Box::new(TrieProofBranch {
                children: [
                    None,
                    None,
                    None,
                    Some(
                        CryptoHash::from_str("7H2oXQXYxnuB3Kq6djTnS7rkTdsgmLhByL3JFHLsrFQM")
                            .unwrap()
                    ),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None
                ],
                value: None,
                memory_usage: 268,
                index: 1
            })),
            TrieProofItem::Leaf(Box::new(TrieProofLeaf {
                key: vec![32, 50, 51],
                value_length: 3,
                value_hash: CryptoHash::from_str("CCYa6DM7NuL4iPoSM4PvtYMRcHFydKhD5oRSXx3jfxCS")
                    .unwrap(),
                memory_usage: 109
            })),
        ]
    );
    // TODO: understand why only when prefix is test123 the proof is not empty. Seems like there's an issue on the implementation
    // of proof retrieval
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
