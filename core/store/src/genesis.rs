//! This file contains helper functions for creating genesis data in DB
//! initialize_genesis_state is currently called by nightshade runtime but should be moved to chain

use rayon::prelude::*;
use std::{collections::HashSet, fs, path::Path};

use borsh::BorshDeserialize;
use near_chain_configs::Genesis;
use near_primitives::{
    epoch_manager::EpochConfig,
    runtime::fees::StorageUsageConfig,
    shard_layout::{account_id_to_shard_id, ShardLayout},
    state_record::{state_record_to_account_id, StateRecord},
    types::{AccountId, ShardId, StateRoot},
};
use tracing::{error, info, warn};

use crate::{
    flat::FlatStorageManager, genesis_state_applier::GenesisStateApplier, ShardTries, Store,
    TrieConfig,
};

const STATE_DUMP_FILE: &str = "state_dump";
const GENESIS_ROOTS_FILE: &str = "genesis_roots";

pub fn initialize_genesis_state(
    store: Store,
    home_dir: &Path,
    config: &StorageUsageConfig,
    genesis: &Genesis,
) -> Vec<StateRoot> {
    let has_dump = home_dir.join(STATE_DUMP_FILE).exists();
    if has_dump {
        if genesis.records_len().is_ok() {
            warn!(target: "store", "Found both records in genesis config and the state dump file. Will ignore the records.");
        }
        genesis_state_from_dump(store, home_dir)
    } else {
        genesis_state_from_records(store, config, genesis)
    }
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

fn genesis_state_from_records(
    store: Store,
    config: &StorageUsageConfig,
    genesis: &Genesis,
) -> Vec<StateRoot> {
    match genesis.records_len() {
        Ok(count) => {
            info!(
                target: "store",
                "genesis state has {count} records, computing state roots"
            )
        }
        Err(path) => {
            info!(
                target: "store",
                path=%path.display(),
                message="computing state roots from records",
            )
        }
    }
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
                config,
                genesis,
                shard_account_ids[shard_id as usize].clone(),
            )
        })
        .collect()
}

fn state_record_to_shard_id(state_record: &StateRecord, shard_layout: &ShardLayout) -> ShardId {
    account_id_to_shard_id(state_record_to_account_id(state_record), shard_layout)
}
