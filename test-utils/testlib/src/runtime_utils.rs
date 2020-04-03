use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};

use near_chain_configs::Genesis;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::serialize::to_base64;
use near_primitives::state_record::StateRecord;
use near_primitives::types::{AccountId, MerkleHash, StateRoot};
use near_primitives::views::AccountView;
use near_store::test_utils::create_trie;
use near_store::{Trie, TrieUpdate};
use neard::config::GenesisExt;
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

const DEFAULT_TEST_CONTRACT: &[u8] =
    include_bytes!("../../../runtime/near-vm-runner/tests/res/test_contract_rs.wasm");

lazy_static::lazy_static! {
    static ref DEFAULT_TEST_CONTRACT_BASE64: String = to_base64(&DEFAULT_TEST_CONTRACT);
    static ref DEFAULT_TEST_CONTRACT_HASH: CryptoHash = hash(&DEFAULT_TEST_CONTRACT);
}

pub fn add_test_contract(genesis: &mut Genesis, account_id: &AccountId) {
    let mut is_account_record_found = false;
    for record in genesis.records.as_mut() {
        if let StateRecord::Account { account_id: record_account_id, ref mut account } = record {
            if record_account_id == account_id {
                is_account_record_found = true;
                account.code_hash = *DEFAULT_TEST_CONTRACT_HASH;
            }
        }
    }
    if !is_account_record_found {
        genesis.records.as_mut().push(StateRecord::Account {
            account_id: account_id.clone(),
            account: AccountView {
                amount: 0,
                locked: 0,
                code_hash: *DEFAULT_TEST_CONTRACT_HASH,
                storage_usage: 0,
                storage_paid_at: 0,
            },
        });
    }
    genesis.records.as_mut().push(StateRecord::Contract {
        account_id: account_id.clone(),
        code: DEFAULT_TEST_CONTRACT_BASE64.clone(),
    });
}

pub fn get_runtime_and_trie_from_genesis(genesis: &Genesis) -> (Runtime, Arc<Trie>, StateRoot) {
    let trie = create_trie();
    let runtime = Runtime::new(genesis.config.runtime_config.clone());
    let trie_update = TrieUpdate::new(trie.clone(), MerkleHash::default());
    let (store_update, genesis_root) = runtime.apply_genesis_state(
        trie_update,
        &genesis
            .config
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
        &genesis.records.as_ref(),
    );
    store_update.commit().unwrap();
    (runtime, trie, genesis_root)
}

pub fn get_runtime_and_trie() -> (Runtime, Arc<Trie>, StateRoot) {
    let mut genesis = Genesis::test(vec![&alice_account(), &bob_account(), "carol.near"], 3);
    add_test_contract(&mut genesis, &AccountId::from("test.contract"));
    get_runtime_and_trie_from_genesis(&genesis)
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
