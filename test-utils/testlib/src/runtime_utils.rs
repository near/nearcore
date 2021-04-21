use byteorder::{ByteOrder, LittleEndian};

use near_chain_configs::Genesis;
use near_primitives::account::Account;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state_record::StateRecord;
use near_primitives::types::{AccountId, StateRoot};
use near_store::test_utils::create_tries;
use near_store::{ShardTries, TrieUpdate};
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
pub fn evm_account() -> AccountId {
    "evm".to_string()
}

pub fn implicit_account() -> AccountId {
    "3885505359911f2493f0c40a2bf042981936ec5dddd59708581b155a047864d8".to_string()
}

lazy_static::lazy_static! {
    static ref DEFAULT_TEST_CONTRACT_HASH: CryptoHash = hash(near_test_contracts::rs_contract());
}

pub fn add_test_contract(genesis: &mut Genesis, account_id: &AccountId) {
    let mut is_account_record_found = false;
    for record in genesis.records.as_mut() {
        if let StateRecord::Account { account_id: record_account_id, ref mut account } = record {
            if record_account_id == account_id {
                is_account_record_found = true;
                account.set_code_hash(*DEFAULT_TEST_CONTRACT_HASH);
            }
        }
    }
    if !is_account_record_found {
        genesis.records.as_mut().push(StateRecord::Account {
            account_id: account_id.clone(),
            account: Account::new(0, 0, *DEFAULT_TEST_CONTRACT_HASH, 0),
        });
    }
    genesis.records.as_mut().push(StateRecord::Contract {
        account_id: account_id.clone(),
        code: near_test_contracts::rs_contract().to_vec(),
    });
}

pub fn get_runtime_and_trie_from_genesis(genesis: &Genesis) -> (Runtime, ShardTries, StateRoot) {
    let tries = create_tries();
    let runtime = Runtime::new();
    let (store_update, genesis_root) = runtime.apply_genesis_state(
        tries.clone(),
        0,
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
        &genesis.config.runtime_config,
    );
    store_update.commit().unwrap();
    (runtime, tries, genesis_root)
}

pub fn get_runtime_and_trie() -> (Runtime, ShardTries, StateRoot) {
    let mut genesis = Genesis::test(vec![&alice_account(), &bob_account(), "carol.near"], 3);
    add_test_contract(&mut genesis, &AccountId::from("test.contract"));
    get_runtime_and_trie_from_genesis(&genesis)
}

pub fn get_test_trie_viewer() -> (TrieViewer, TrieUpdate) {
    let (_, tries, root) = get_runtime_and_trie();
    let trie_viewer = TrieViewer::new_with_state_size_limit(None);
    let state_update = tries.new_trie_update(0, root);
    (trie_viewer, state_update)
}

pub fn encode_int(val: i32) -> [u8; 4] {
    let mut tmp = [0u8; 4];
    LittleEndian::write_i32(&mut tmp, val);
    tmp
}
