use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};
use near::GenesisConfig;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::types::{AccountId, MerkleHash, StateRoot};
use near_store::test_utils::create_trie;
use near_store::{Trie, TrieUpdate};
use node_runtime::{state_viewer::TrieViewer, Runtime};

pub fn alice_account() -> AccountId {
    "alice.near".to_string()
}
pub fn bob_account() -> AccountId {
    "bob.near".to_string()
}
pub fn eve_dot_alice_account() -> AccountId {
    "eve.alice.near".to_string()
}

pub fn default_code_hash() -> CryptoHash {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("../../runtime/near-vm-runner/tests/res/test_contract_rs.wasm");
    let genesis_wasm = fs::read(path).unwrap();
    hash(&genesis_wasm)
}

pub fn get_runtime_and_trie_from_genesis(
    genesis_config: &GenesisConfig,
) -> (Runtime, Arc<Trie>, StateRoot) {
    let trie = create_trie();
    let runtime = Runtime::new(genesis_config.runtime_config.clone());
    let trie_update = TrieUpdate::new(trie.clone(), MerkleHash::default());
    let (store_update, genesis_root) = runtime.apply_genesis_state(
        trie_update,
        &genesis_config
            .validators
            .iter()
            .map(|account_info| {
                (
                    account_info.account_id.clone(),
                    account_info.public_key.clone(),
                    account_info.amount,
                )
            })
            .collect::<Vec<_>>(),
        &genesis_config.records.clone(),
    );
    store_update.commit().unwrap();
    (runtime, trie, genesis_root)
}

pub fn get_runtime_and_trie() -> (Runtime, Arc<Trie>, StateRoot) {
    let genesis_config =
        GenesisConfig::test(vec![&alice_account(), &bob_account(), "carol.near"], 3);
    get_runtime_and_trie_from_genesis(&genesis_config)
}

pub fn get_test_trie_viewer() -> (TrieViewer, TrieUpdate) {
    let (_, trie, root) = get_runtime_and_trie();
    let trie_viewer = TrieViewer::new();
    let state_update = TrieUpdate::new(trie, root);
    (trie_viewer, state_update)
}

pub fn encode_int(val: i32) -> [u8; 4] {
    let mut tmp = [0u8; 4];
    LittleEndian::write_i32(&mut tmp, val);
    tmp
}
