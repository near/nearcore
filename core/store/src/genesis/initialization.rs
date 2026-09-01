//! This file contains helper functions for initialization of genesis data in store
//! We first check if store has the genesis state_roots, if not, we go ahead with initialization

use crate::adapter::StoreAdapter;
use crate::flat::FlatStorageManager;
use crate::genesis::GenesisStateApplier;
use crate::{
    ShardTries, StateSnapshotConfig, Store, TrieConfig, get_genesis_height,
    get_genesis_state_roots, set_genesis_height, set_genesis_state_roots,
};
use borsh::BorshDeserialize;
use near_chain_configs::{Genesis, GenesisContents};
use near_parameters::RuntimeConfigStore;
use near_primitives::chains::{MAINNET, TESTNET};
use near_primitives::epoch_manager::EpochConfig;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::{
    StateRecord, state_record_to_account_id, state_record_to_shard_id,
};
use near_primitives::types::{AccountId, ShardId, StateRoot};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;

const STATE_DUMP_FILE: &str = "state_dump";
const GENESIS_ROOTS_FILE: &str = "genesis_roots";

pub fn initialize_sharded_genesis_state(
    store: Store,
    genesis: &Genesis,
    // The layout genesis state is built under. Callers that have an EpochManager must pass the
    // layout recorded in the genesis EpochInfo, so the two cannot disagree; `initialize_genesis_state`
    // resolves it the same way the EpochManager would.
    shard_layout: &ShardLayout,
    home_dir: Option<&Path>,
) {
    let state_roots = if let Some(state_roots) = get_genesis_state_roots(&store) {
        check_state_roots_match_layout(&state_roots, shard_layout, "already stored");
        // TODO: with 2.6 release, remove storing genesis height
        let mut store_update: crate::StoreUpdate = store.store_update();
        set_genesis_height(&mut store_update, &genesis.config.genesis_height);
        store_update.commit();

        let genesis_height =
            get_genesis_height(&store).expect("Genesis height not found in storage");
        assert_eq!(
            genesis_height, genesis.config.genesis_height,
            "Genesis height in store is different from the one in genesis config"
        );
        state_roots
    } else {
        let has_dump = home_dir.is_some_and(|dir| dir.join(STATE_DUMP_FILE).exists());
        let (state_roots, source) = if has_dump {
            if let GenesisContents::Records { .. } = &genesis.contents {
                tracing::warn!(target: "store", "found both records in genesis config and the state dump file, will ignore the records");
            }
            (genesis_state_from_dump(store.clone(), home_dir.unwrap()), "from the state dump")
        } else {
            (genesis_state_from_genesis(store.clone(), genesis, shard_layout), "in genesis")
        };
        // Before committing, not after: the genesis EpochInfo has already been written by the
        // EpochManager by the time we get here, so failing after `set_genesis_state_roots` would
        // leave a half-initialized DB that is never re-derived on restart and can only be wiped.
        check_state_roots_match_layout(&state_roots, shard_layout, source);
        let mut store_update = store.store_update();
        set_genesis_state_roots(&mut store_update, &state_roots);
        set_genesis_height(&mut store_update, &genesis.config.genesis_height);
        store_update.commit();
        state_roots
    };

    // Some hardcoded checks for mainnet and testnet
    if &genesis.config.chain_id == MAINNET {
        assert_eq!(format!("{state_roots:?}"), "[8EhZRfDTYujfZoUZtZ3eSMB9gJyFo5zjscR12dEcaxGU]");
    }

    if &genesis.config.chain_id == TESTNET {
        assert_eq!(format!("{state_roots:?}"), "[7EAgMRCrBWcb3ZS6SZJ7Dm71VZ1jaBpgGiewAEvFqPT1]");
    }
}

/// The shard layout and the genesis state roots have to agree on how many shards there are.
///
/// This is a real cross-check only where the roots do not come from the layout: state roots given
/// directly in genesis, or loaded from a state dump. When they are computed from records they are
/// derived per shard uid and the counts agree by construction.
///
/// It is worth checking on every path regardless, because `genesis_chunks` silently replicates a
/// single root across every shard when the counts disagree, which yields a chain that starts and
/// is wrong rather than one that refuses to start.
fn check_state_roots_match_layout(
    state_roots: &[StateRoot],
    shard_layout: &ShardLayout,
    source: &str,
) {
    assert_eq!(
        state_roots.len(),
        shard_layout.num_shards() as usize,
        "genesis state has {} state roots ({source}) but the genesis shard layout has {} shards. \
         If the epoch config for the genesis protocol version declares no static shard layout, \
         genesis.config.shard_layout must describe the state - note it defaults to a single shard \
         when the field is omitted.",
        state_roots.len(),
        shard_layout.num_shards(),
    );
}

pub fn initialize_genesis_state(store: Store, genesis: &Genesis, home_dir: Option<&Path>) {
    let epoch_config = EpochConfig::from(&genesis.config);
    let shard_layout =
        epoch_config.static_shard_layout().unwrap_or_else(|| genesis.config.shard_layout.clone());
    initialize_sharded_genesis_state(store, genesis, &shard_layout, home_dir);
}

fn genesis_state_from_dump(store: Store, home_dir: &Path) -> Vec<StateRoot> {
    tracing::error!(target: "near", "loading genesis from a state dump file, do not use this outside of genesis-tools");
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

fn genesis_state_from_genesis(
    store: Store,
    genesis: &Genesis,
    shard_layout: &ShardLayout,
) -> Vec<StateRoot> {
    match &genesis.contents {
        GenesisContents::Records { records } => {
            tracing::info!(
                target: "runtime",
                num_records = records.0.len(),
                "genesis state has records, computing state roots"
            )
        }
        GenesisContents::RecordsFile { records_file } => {
            tracing::info!(
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
    let shard_ids: Vec<_> = shard_layout.shard_ids().collect();
    let shard_uids: Vec<_> = shard_layout.shard_uids().collect();

    let mut shard_account_ids: BTreeMap<ShardId, HashSet<AccountId>> =
        shard_ids.iter().map(|&shard_id| (shard_id, HashSet::new())).collect();
    let mut has_protocol_account = false;
    tracing::info!(target: "store","distributing records to shards");

    genesis.for_each_record(|record: &StateRecord| {
        let account_id = state_record_to_account_id(record).clone();
        if !account_id.is_system() {
            let shard_id = state_record_to_shard_id(record, &shard_layout);
            shard_account_ids.get_mut(&shard_id).unwrap().insert(account_id);
        }
        if let StateRecord::Account { account_id, .. } = record {
            if account_id == &genesis.config.protocol_treasury_account {
                has_protocol_account = true;
            }
        }
    });
    assert!(has_protocol_account, "Genesis spec doesn't have protocol treasury account");
    let tries = ShardTries::new(
        store.trie_store(),
        TrieConfig::default(),
        FlatStorageManager::new(store.flat_store()),
        StateSnapshotConfig::Disabled,
    );

    let writers = std::sync::atomic::AtomicUsize::new(0);
    shard_uids
        .into_par_iter()
        .map(|shard_uid| {
            let shard_id = shard_uid.shard_id();
            let validators = genesis
                .config
                .validators
                .iter()
                .filter_map(|account_info| {
                    if shard_layout.account_id_to_shard_id(&account_info.account_id) == shard_id {
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
                shard_uid,
                &validators,
                storage_usage_config,
                genesis,
                shard_account_ids[&shard_id].clone(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::initialize_sharded_genesis_state;
    use crate::genesis::initialization::get_genesis_state_roots;
    use crate::test_utils::create_test_store;
    use near_chain_configs::{Genesis, GenesisConfig, GenesisContents};
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardLayout;

    /// Genesis whose state roots are given directly, so they are independent of the shard layout
    /// and the two can actually disagree. This is the shape a forknet image uses.
    fn genesis_with_state_roots(num_roots: usize) -> Genesis {
        let mut config = GenesisConfig::default();
        config.chain_id = "test-dynamic-genesis".to_string();
        config.genesis_height = 1;
        Genesis {
            config,
            contents: GenesisContents::StateRoots {
                state_roots: (0..num_roots).map(|i| CryptoHash::hash_bytes(&[i as u8])).collect(),
            },
        }
    }

    /// A layout the epoch config does not declare - it can only have come from genesis - is
    /// accepted when it describes the state that is actually there.
    #[test]
    fn genesis_shard_layout_matching_state_roots_is_accepted() {
        let shard_layout = ShardLayout::multi_shard(4, 3);
        let genesis = genesis_with_state_roots(shard_layout.num_shards() as usize);
        let store = create_test_store();

        initialize_sharded_genesis_state(store.clone(), &genesis, &shard_layout, None);

        let roots = get_genesis_state_roots(&store).unwrap();
        assert_eq!(roots.len(), shard_layout.num_shards() as usize);
    }

    /// The case the check exists for: `genesis.config.shard_layout` is `#[serde(default)]`, so a
    /// genesis file that omits it silently claims a single shard. Against real multi-shard state
    /// that must fail loudly rather than start a wrong chain.
    #[test]
    #[should_panic(expected = "state roots")]
    fn defaulted_single_shard_layout_against_multi_shard_state_is_rejected() {
        let genesis = genesis_with_state_roots(4);
        let store = create_test_store();

        initialize_sharded_genesis_state(store, &genesis, &ShardLayout::single_shard(), None);
    }

    /// Nothing is written when validation fails, so a corrected config works on the next attempt
    /// instead of needing the DB wiped.
    #[test]
    fn nothing_is_committed_when_validation_fails() {
        let genesis = genesis_with_state_roots(4);
        let store = create_test_store();

        // AssertUnwindSafe: the store is only read after the unwind, to check nothing landed.
        let bad = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            initialize_sharded_genesis_state(
                store.clone(),
                &genesis,
                &ShardLayout::single_shard(),
                None,
            )
        }));
        assert!(bad.is_err(), "mismatched layout should have been rejected");
        assert!(
            get_genesis_state_roots(&store).is_none(),
            "genesis state roots must not be committed when the layout does not match"
        );

        // Same store, corrected layout: succeeds without any manual cleanup.
        initialize_sharded_genesis_state(
            store.clone(),
            &genesis,
            &ShardLayout::multi_shard(4, 3),
            None,
        );
        assert_eq!(get_genesis_state_roots(&store).unwrap().len(), 4);
    }
}
