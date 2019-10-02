//! Tools for creating a genesis block.

use crate::{get_store_path, GenesisConfig, NightshadeRuntime};
use near_chain::{Block, ChainStore, RuntimeAdapter, Tip};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::account::AccessKey;
use near_primitives::contract::ContractCode;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::serialize::to_base64;
use near_primitives::types::{Balance, ChunkExtra};
use near_primitives::views::AccountView;
use near_store::{
    create_store, get_account, set_access_key, set_account, set_code, Store, TrieUpdate,
};
use node_runtime::StateRecord;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

fn get_account_id(account_index: u64) -> String {
    format!("near_{}", account_index)
}

pub fn create_genesis_block_with_additional_accounts(
    home_dir: &Path,
    config: GenesisConfig,
    additional_accounts_num: u64,
) {
    let store = create_store(&get_store_path(home_dir));
    create_genesis_block_with_additional_accounts_w_store(
        home_dir,
        config,
        store,
        additional_accounts_num,
    );
}

/// Creates genesis block in the given storage.
/// If `additional_accounts_num` is specified generates accounts with small smart contracts
/// # Args:
/// `home_dir` -- used by ethash only;
/// `config` -- `GenesisConfig`;
/// `store` -- on-disk or in-memory storage object;
/// `additional_accounts_num` -- approximate number of accounts to additionally generate per shard.
/// For testing only;
pub fn create_genesis_block_with_additional_accounts_w_store(
    home_dir: &Path,
    config: GenesisConfig,
    store: Arc<Store>,
    additional_accounts_num: u64,
) {
    let runtime = NightshadeRuntime::new(
        home_dir,
        store.clone(),
        config.clone(),
        // Since we are not using runtime as an actor
        // there is no reason to track accounts or shards.
        vec![],
        vec![],
    );
    let (store_update, mut roots) = runtime.genesis_state();
    store_update.commit().unwrap();

    // Prepare code that we are going to deploy to each account.
    let wasm_binary: &[u8] =
        include_bytes!("../../runtime/runtime/tests/tiny-contract-rs/res/tiny_contract_rs.wasm");
    let wasm_binary_base64 = to_base64(wasm_binary);
    let code_hash = hash(wasm_binary);

    // Keep state update per each shard.
    let mut state_updates: HashMap<_, _> = roots
        .iter()
        .map(|root| TrieUpdate::new(runtime.trie.clone(), root.clone()))
        .enumerate()
        .collect();
    // Keep track of records we have added per shard and have not flushed yet.
    let mut unflushed_records: Vec<Vec<_>> = vec![vec![]; runtime.num_shards() as _];
    // A convenience macro to flush records into the DB, while also computing the storage usage.
    macro_rules! flush_shard {
        ($shard_id: expr) => {
            if unflushed_records[$shard_id as usize].is_empty() {
                return;
            }
            let records = &mut unflushed_records[$shard_id as usize];
            let mut state_update = state_updates.remove(&($shard_id as usize)).unwrap();
            // Compute storage usage and update accounts.
            for (account_id, storage_usage) in runtime.runtime.compute_storage_usage(records) {
                let mut account = get_account(&state_update, &account_id)
                    .expect("Genesis storage error")
                    .expect("Account must exist");
                account.storage_usage = storage_usage;
                set_account(&mut state_update, &account_id, &account);
            }
            let trie = state_update.trie.clone();
            let (store_update, root) = state_update
                .finalize()
                .expect("Genesis state update failed")
                .into(trie)
                .expect("Genesis state update failed");
            store_update.commit().unwrap();

            roots[$shard_id as usize] = root;
            records.clear();
            *state_updates.get_mut(&($shard_id as usize)).unwrap() =
                TrieUpdate::new(runtime.trie.clone(), root);
        };
    };

    // Add records in chunks of 3000 per shard for memory efficiency reasons.
    const CHUNK_SIZE: usize = 3000;
    const TESTING_INIT_BALANCE: Balance = 1_000_000_000_000_000;
    const TESTING_INIT_STAKE: Balance = 50_000_000;
    for i in 0..additional_accounts_num * runtime.num_shards() {
        let account_id = get_account_id(i);
        let shard_id = runtime.account_id_to_shard_id(&account_id);
        let records = &mut unflushed_records[shard_id as usize];
        let state_update = state_updates.get_mut(&(shard_id as usize)).unwrap();

        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let account = AccountView {
            amount: TESTING_INIT_BALANCE,
            staked: TESTING_INIT_STAKE,
            code_hash: code_hash.clone().into(),
            storage_usage: 0,
            storage_paid_at: 0,
        };
        set_account(state_update, &account_id, &account.clone().into());
        let account_record = StateRecord::Account { account_id: account_id.clone(), account };
        records.push(account_record);
        let access_key_record = StateRecord::AccessKey {
            account_id: account_id.clone(),
            public_key: signer.public_key.clone().into(),
            access_key: AccessKey::full_access().into(),
        };
        set_access_key(state_update, &account_id, &signer.public_key, &AccessKey::full_access());
        records.push(access_key_record);
        let code = ContractCode::new(wasm_binary.to_vec());
        set_code(state_update, &account_id, &code);
        let contract_record = StateRecord::Contract {
            account_id: account_id.clone(),
            code: wasm_binary_base64.clone(),
        };
        records.push(contract_record);

        if records.len() >= CHUNK_SIZE {
            flush_shard!(shard_id);
        }
    }

    for i in 0..runtime.num_shards() {
        flush_shard!(i);
    }

    // Finally, create genesis block and add it to the storage.
    let genesis = Block::genesis(
        roots.clone(),
        config.genesis_time.clone(),
        runtime.num_shards(),
        config.gas_limit.clone(),
        config.gas_price.clone(),
        config.total_supply.clone(),
    );

    let mut store = ChainStore::new(store);
    let mut store_update = store.store_update();

    runtime
        .add_validator_proposals(
            CryptoHash::default(),
            genesis.hash(),
            genesis.header.inner.height,
            vec![],
            vec![],
            vec![],
            0,
            config.gas_price.clone(),
            0,
            config.total_supply.clone(),
        )
        .unwrap();
    store_update.save_block_header(genesis.header.clone());
    store_update.save_block(genesis.clone());

    for (chunk_header, state_root) in genesis.chunks.iter().zip(roots.iter()) {
        store_update.save_chunk_extra(
            &genesis.hash(),
            chunk_header.inner.shard_id,
            ChunkExtra::new(state_root, vec![], 0, config.gas_limit.clone(), 0),
        );
    }

    let head = Tip::from_header(&genesis.header);
    store_update.save_head(&head).unwrap();
    store_update.save_sync_head(&head);
    store_update.commit().unwrap();
}
