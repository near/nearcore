//! This file contains helper functions for initialization of genesis data in store
//! We first check if store has the genesis hash and state_roots, if not, we go ahead with initialization

use rayon::prelude::*;
use std::{collections::HashSet, fs, path::Path};

use borsh::BorshDeserialize;
use near_chain_configs::{Genesis, GenesisContents};
use near_primitives::{
    epoch_manager::EpochConfig,
    runtime::config_store::RuntimeConfigStore,
    shard_layout::{account_id_to_shard_id, ShardLayout},
    state_record::{state_record_to_account_id, StateRecord},
    types::{AccountId, NumShards, ShardId, StateRoot},
};
use tracing::{error, info, warn};

use crate::{
    flat::FlatStorageManager, genesis::GenesisStateApplier, get_genesis_hash,
    get_genesis_state_roots, set_genesis_hash, set_genesis_state_roots, ShardTries, Store,
    TrieConfig,
};

const STATE_DUMP_FILE: &str = "state_dump";
const GENESIS_ROOTS_FILE: &str = "genesis_roots";

pub fn initialize_genesis_state(store: Store, genesis: &Genesis, home_dir: Option<&Path>) {
    // Ignore initialization if we already have genesis hash and state roots in store
    let stored_hash = get_genesis_hash(&store).expect("Store failed on genesis intialization");
    if let Some(_hash) = stored_hash {
        // TODO: re-enable this check (#4447)
        //assert_eq!(hash, genesis_hash, "Storage already exists, but has a different genesis");
        get_genesis_state_roots(&store)
            .expect("Store failed on genesis intialization")
            .expect("Genesis state roots not found in storage");
        return;
    } else {
        let has_dump = home_dir.is_some_and(|dir| dir.join(STATE_DUMP_FILE).exists());
        let state_roots = if has_dump {
            if let GenesisContents::Records { .. } = &genesis.contents {
                warn!(target: "store", "Found both records in genesis config and the state dump file. Will ignore the records.");
            }
            genesis_state_from_dump(store.clone(), home_dir.unwrap())
        } else {
            genesis_state_from_genesis(store.clone(), genesis)
        };
        let genesis_hash = genesis.json_hash();
        let mut store_update = store.store_update();
        set_genesis_hash(&mut store_update, &genesis_hash);
        set_genesis_state_roots(&mut store_update, &state_roots);
        store_update.commit().expect("Store failed on genesis intialization");
    }

    let genesis_config = &genesis.config;
    assert_eq!(
        genesis_config.shard_layout.num_shards(),
        genesis_config.num_block_producer_seats_per_shard.len() as NumShards,
        "genesis config shard_layout and num_block_producer_seats_per_shard indicate inconsistent number of shards {} vs {}",
        genesis_config.shard_layout.num_shards(),
        genesis_config.num_block_producer_seats_per_shard.len() as NumShards,
    );
}

fn genesis_state_from_dump(store: Store, home_dir: &Path) -> Vec<StateRoot> {
    error!(target: "near", "Loading genesis from a state dump file. Do not use this outside of genesis-tools");
    let mut state_file = home_dir.to_path_buf();
    state_file.push(STATE_DUMP_FILE);
    store.load_state_from_file(state_file.as_path()).expect("Failed to read state dump");
    let mut roots_files = home_dir.to_path_buf();
    roots_files.push(GENESIS_ROOTS_FILE);
    let data = fs::read(roots_files).expect("Failed to read genesis roots file.");
    let state_roots: Vec<StateRoot> =
        BorshDeserialize::try_from_slice(&data).expect("Failed to deserialize genesis roots");
    state_roots
}

fn genesis_state_from_genesis(store: Store, genesis: &Genesis) -> Vec<StateRoot> {
    match &genesis.contents {
        GenesisContents::Records { records } => {
            info!(
                target: "runtime",
                "genesis state has {} records, computing state roots",
                records.0.len(),
            )
        }
        GenesisContents::RecordsFile { records_file } => {
            info!(
                target: "runtime",
                path=%records_file.display(),
                message="computing state roots from records",
            )
        }
        GenesisContents::StateRoots { state_roots } => {
            return state_roots.clone();
        }
    }
    let runtime_config_store = RuntimeConfigStore::for_chain_id(&genesis.config.chain_id);
    let runtime_config = runtime_config_store.get_config(genesis.config.protocol_version);
    let storage_usage_config = &runtime_config.fees.storage_usage_config;
    let initial_epoch_config = EpochConfig::from(&genesis.config);
    let shard_layout = initial_epoch_config.shard_layout;
    let num_shards = shard_layout.num_shards();
    let mut shard_account_ids: Vec<HashSet<AccountId>> =
        (0..num_shards).map(|_| HashSet::new()).collect();
    let mut has_protocol_account = false;
    info!(target: "store","distributing records to shards");

    genesis.for_each_record(|record: &StateRecord| {
        shard_account_ids[state_record_to_shard_id(record, &shard_layout) as usize]
            .insert(state_record_to_account_id(record).clone());
        if let StateRecord::Account { account_id, .. } = record {
            if account_id == &genesis.config.protocol_treasury_account {
                has_protocol_account = true;
            }
        }
    });
    assert!(has_protocol_account, "Genesis spec doesn't have protocol treasury account");
    let tries = ShardTries::new(
        store.clone(),
        TrieConfig::default(),
        &genesis.config.shard_layout.get_shard_uids(),
        FlatStorageManager::new(store),
    );

    let writers = std::sync::atomic::AtomicUsize::new(0);
    (0..num_shards)
        .into_par_iter()
        .map(|shard_id| {
            let validators = genesis
                .config
                .validators
                .iter()
                .filter_map(|account_info| {
                    if account_id_to_shard_id(&account_info.account_id, &shard_layout) == shard_id {
                        Some((
                            account_info.account_id.clone(),
                            account_info.public_key.clone(),
                            account_info.amount,
                        ))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            GenesisStateApplier::apply(
                &writers,
                tries.clone(),
                shard_id,
                &validators,
                storage_usage_config,
                genesis,
                shard_account_ids[shard_id as usize].clone(),
            )
        })
        .collect()
}

fn state_record_to_shard_id(state_record: &StateRecord, shard_layout: &ShardLayout) -> ShardId {
    account_id_to_shard_id(state_record_to_account_id(state_record), shard_layout)
}
