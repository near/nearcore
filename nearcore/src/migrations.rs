#[cfg(feature = "protocol_feature_flat_state")]
use crate::NightshadeRuntime;
use borsh::BorshSerialize;
use crossbeam_channel::unbounded;
use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::receipt::ReceiptResult;
use near_primitives::runtime::migration_data::MigrationData;
use near_primitives::state::ValueRef;
use near_primitives::state_record::StateRecord;
use near_primitives::types::Gas;
use near_primitives::utils::index_to_bytes;
use near_store::metadata::{DbVersion, DB_VERSION};
use near_store::migrations::BatchedStoreUpdate;
use near_store::{DBCol, NibbleSlice, NodeStorage, Store, Temperature, Trie, TrieIterator};
use std::sync::Arc;
#[cfg(feature = "protocol_feature_flat_state")]
use tracing::{debug, info};

/// Fix an issue with block ordinal (#5761)
// This migration takes at least 3 hours to complete on mainnet
pub fn migrate_30_to_31(
    storage: &NodeStorage,
    near_config: &crate::NearConfig,
) -> anyhow::Result<()> {
    let store = storage.get_store(Temperature::Hot);
    if near_config.client_config.archive && &near_config.genesis.config.chain_id == "mainnet" {
        do_migrate_30_to_31(&store, &near_config.genesis.config)?;
    }
    Ok(())
}

/// Migrates the database from version 30 to 31.
///
/// Recomputes block ordinal due to a bug fixed in #5761.
pub fn do_migrate_30_to_31(
    store: &near_store::Store,
    genesis_config: &near_chain_configs::GenesisConfig,
) -> anyhow::Result<()> {
    let genesis_height = genesis_config.genesis_height;
    let chain_store = ChainStore::new(store.clone(), genesis_height, false);
    let head = chain_store.head()?;
    let mut store_update = BatchedStoreUpdate::new(&store, 10_000_000);
    let mut count = 0;
    // we manually checked mainnet archival data and the first block where the discrepancy happened is `47443088`.
    for height in 47443088..=head.height {
        if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
            let block_ordinal = chain_store.get_block_merkle_tree(&block_hash)?.size();
            let block_hash_from_block_ordinal =
                chain_store.get_block_hash_from_ordinal(block_ordinal)?;
            if block_hash_from_block_ordinal != block_hash {
                println!(
                    "Inconsistency in block ordinal to block hash mapping found at block height {}",
                    height
                );
                count += 1;
                store_update
                    .set_ser(DBCol::BlockOrdinal, &index_to_bytes(block_ordinal), &block_hash)
                    .expect("BorshSerialize should not fail");
            }
        }
    }
    println!("total inconsistency count: {}", count);
    store_update.finish()?;
    Ok(())
}

/// Migrates database from version 34 to 35.
///
/// It is expected to run against a node without flat storage and should fill the flat storage
/// columns with state data related to last final head. In other words, previously used binary
/// should be built without `protocol_feature_flat_state` feature and new binary should include it.
/// Don't use in production, currently used for testing and estimation purposes.
#[cfg(feature = "protocol_feature_flat_state")]
pub fn migrate_34_to_35(
    storage: &NodeStorage,
    near_config: &crate::NearConfig,
) -> anyhow::Result<()> {
    Ok(())
    let store = storage.get_store(Temperature::Hot);
    // just needed for NightshadeRuntime initialization and is used only for trying to retrieve state dump which is
    // not present. we should consider making this parameter optional or pass homedir to migrator
    let tmpdir = tempfile::Builder::new().prefix("storage").tempdir().unwrap();
    let runtime = NightshadeRuntime::from_config(tmpdir.path(), store.clone(), &near_config);
    do_migrate_34_to_35(runtime, &near_config.genesis.config)?;
    Ok(())
}

#[cfg(feature = "protocol_feature_flat_state")]
pub fn do_migrate_34_to_35(
    runtime: NightshadeRuntime,
    genesis_config: &near_chain_configs::GenesisConfig,
) -> anyhow::Result<()> {
    let store = runtime.store();
    let mut chain_store = ChainStore::new(store.clone(), genesis_config.genesis_height, false);
    let final_head = chain_store.final_head()?;
    let block_hash = final_head.last_block_hash;
    let epoch_id = runtime.get_epoch_id(&block_hash)?;
    let num_shards = runtime.num_shards(&epoch_id)?;
    let mut store_update = BatchedStoreUpdate::new(&store, 1_000);
    info!(target: "chain", "Writing flat state heads");
    for shard_id in 0..num_shards {
        store_update
            .set_ser(crate::DBCol::FlatStateMisc, &shard_id.try_to_vec().unwrap(), &block_hash)
            .expect("Error writing flat head from storage");
    }
    store_update.finish()?;

    // DEBUG SPECIFIC KEY
    // let x = String::from("01302e636c69656e742e726");
    // let x_nibbles: Vec<_> = x.chars().map(|c| char::to_digit(c, 16).unwrap() as u8).collect();
    // let shard_uid = runtime.shard_id_to_uid(0, &epoch_id)?;
    // let state_root = chain_store.get_chunk_extra(&block_hash, &shard_uid)?.state_root().clone();
    // let trie = runtime.get_trie_for_shard(0, &block_hash, state_root, false)?;
    // let mut trie_iter = trie.iter()?;
    // let path_begin_encoded = NibbleSlice::encode_nibbles(&x_nibbles, false);
    // trie_iter.seek_nibble_slice(NibbleSlice::from_encoded(&path_begin_encoded).0, true)?;
    // trie_iter.for_each(|item| {
    //     let item = item.unwrap();
    //     let sr = StateRecord::from_raw_key_value(item.0, item.1).unwrap();
    //     debug!(target: "store", %sr);
    // });
    // panic!("Test finished");

    // for shard_id in 0..num_shards {
    //     info!(target: "chain", %shard_id, "Start flat state shard migration");
    //     let shard_uid = runtime.shard_id_to_uid(shard_id, &epoch_id)?;
    //     let state_root = chain_store.get_chunk_extra(&block_hash, &shard_uid)?.state_root().clone();
    //     let trie = runtime.get_trie_for_shard(shard_id, &block_hash, state_root, false)?;
    //     let root_node = trie.retrieve_root_node().unwrap();
    //     info!(target: "store", %shard_id, "{} gb", root_node.memory_usage / 10u64.pow(9));
    // }
    //
    // panic!("");

    let pool = rayon::ThreadPoolBuilder::new().num_threads(512).build().unwrap();

    for shard_id in 1..4 {
        let shard_uid = runtime.shard_id_to_uid(shard_id, &epoch_id)?;
        let state_root = chain_store.get_chunk_extra(&block_hash, &shard_uid)?.state_root().clone();
        let trie = runtime.get_trie_for_shard(shard_id, &block_hash, state_root, false)?;
        let root_node = trie.retrieve_root_node().unwrap();
        let memory_usage = root_node.memory_usage;

        let sub_trie_size = memory_usage / 1024; //5_000_000;
        info!(target: "chain", %shard_id, "Start flat state shard migration, mem_usage = {} gb, sub_trie_size = {} gb", memory_usage as f64 / 10f64.powf(9.0), sub_trie_size as f64 / 10f64.powf(9.0));

        let (sender, receiver) = unbounded();

        // let max_threads = 1024;
        let thread_usage = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let mem_progress = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let global_nodes_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut state_iter = trie.iter()?;
        // let x = String::from("09626173656c696e655f73776561742");
        // let x_nibbles: Vec<_> = x.chars().map(|c| char::to_digit(c, 16).unwrap() as u8).collect();
        // let path_begin_encoded = NibbleSlice::encode_nibbles(&x_nibbles, false);
        // state_iter.seek_nibble_slice(NibbleSlice::from_encoded(&path_begin_encoded).0, true)?;
        // let path_begin_encoded = NibbleSlice::encode_nibbles(&[0], false);
        // state_iter.seek_nibble_slice(NibbleSlice::from_encoded(&path_begin_encoded).0, true)?;

        let mut threads = 0u64;
        for sub_trie in state_iter.heavy_sub_tries(sub_trie_size)? {
            let TrieIterator { trie, trail, key_nibbles, .. } = sub_trie?;
            if key_nibbles.len() > 2000 {
                panic!("Too long nibbles vec");
            }
            // DEBUG HEX PREFIXES
            // let hex_prefix: String = key_nibbles
            //     .iter()
            //     .map(|&n| char::from_digit(n as u32, 16).expect("nibble should be <16"))
            //     .collect();
            // debug!(target: "store", "Preload subtrie at {hex_prefix}");

            let storage = trie
                .storage
                .as_caching_storage()
                .expect("preload called without caching storage")
                .clone();
            let root = trie.get_root().clone();
            let inner_mem_progress = mem_progress.clone();
            let inner_thread_usage = thread_usage.clone();
            let inner_nodes_count = global_nodes_count.clone();
            let inner_sender = sender.clone();

            let inner_store = store.clone();
            threads += 1;
            pool.spawn(move || {
                inner_thread_usage.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let hex_prefix: String = key_nibbles
                    .iter()
                    .map(|&n| char::from_digit(n as u32, 16).expect("nibble should be <16"))
                    .collect();
                debug!(target: "store", "Preload subtrie at {hex_prefix}");
                let trie = Trie::new(Box::new(storage), root, None);
                let current_memory_usage = trail.last().unwrap().node.memory_usage();
                let nodes_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
                let inner_iter = TrieIterator {
                    trie: &trie,
                    trail,
                    key_nibbles,
                    visited_nodes: None,
                    nodes_count: nodes_count.clone(),
                    global_nodes_count: inner_nodes_count,
                };
                let mut store_update = BatchedStoreUpdate::new(&inner_store, 10_000_000);
                let mut first_state_record: Option<StateRecord> = None;
                let n = inner_iter
                    .enumerate()
                    .map(|(i, item)| {
                        let item = item.unwrap();
                        let value_ref = ValueRef::new(&item.1);
                        store_update
                            .set_ser(DBCol::FlatState, &item.0, &value_ref)
                            .expect("Failed to put value in FlatState");
                        if i == 0 {
                            first_state_record = StateRecord::from_raw_key_value(item.0, item.1);
                        }
                    })
                    .count();
                store_update.finish().unwrap();
                inner_thread_usage.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                inner_mem_progress
                    .fetch_add(current_memory_usage, std::sync::atomic::Ordering::Relaxed);

                let nodes_count = nodes_count.load(std::sync::atomic::Ordering::Relaxed);
                let threads_usage = inner_thread_usage.load(std::sync::atomic::Ordering::Relaxed);
                let mem_progress_gb = inner_mem_progress.load(std::sync::atomic::Ordering::Relaxed)
                    as f64
                    / 10f64.powf(9.0);
                let mem_usage_gb = memory_usage as f64 / 10f64.powf(9.0);
                let first_record_to_display = match first_state_record {
                    Some(state_record) => format!("{}", state_record),
                    None => String::from(""),
                };
                debug!(target: "store",
                    "Preload subtrie at {hex_prefix} done, \
                    loaded {n:<8} state items, \
                    visited {nodes_count} nodes, \
                    {threads_usage} threads used, \
                    {first_record_to_display} is the first state record, \
                    mem progress gb: {mem_progress_gb} / {mem_usage_gb}"
                );
                inner_sender.send(n).unwrap();
            });
            // handles.push(handle);
        }

        let mut n = 0;
        for _ in 0..threads {
            n += receiver.recv().unwrap();
        }
        // for handle in handles {
        //     n += rt.block_on(handle).expect("task failed");
        // }
        info!(target: "store", "Wrote {n} state items");
    }

    panic!("Migration succeeded. Still, don't start the node yet.");

    Ok(())
}

/// In test runs reads and writes here used 442 TGas, but in test on live net migration take
/// between 4 and 4.5s. We do not want to process any receipts in this block
const GAS_USED_FOR_STORAGE_USAGE_DELTA_MIGRATION: Gas = 1_000_000_000_000_000;

pub fn load_migration_data(chain_id: &str) -> MigrationData {
    let is_mainnet = chain_id == "mainnet";
    MigrationData {
        storage_usage_delta: if is_mainnet {
            near_mainnet_res::mainnet_storage_usage_delta()
        } else {
            Vec::new()
        },
        storage_usage_fix_gas: if is_mainnet {
            GAS_USED_FOR_STORAGE_USAGE_DELTA_MIGRATION
        } else {
            0
        },
        restored_receipts: if is_mainnet {
            near_mainnet_res::mainnet_restored_receipts()
        } else {
            ReceiptResult::default()
        },
    }
}

pub(super) struct Migrator<'a> {
    config: &'a crate::config::NearConfig,
}

impl<'a> Migrator<'a> {
    pub fn new(config: &'a crate::config::NearConfig) -> Self {
        Self { config }
    }
}

/// Asserts that node’s configuration does not use deprecated snapshot config
/// options.
///
/// The `use_db_migration_snapshot` and `db_migration_snapshot_path`
/// configuration options have been deprecated in favour of
/// `store.migration_snapshot`.
///
/// This function panics if the old options are set and shows instruction how to
/// migrate to the new options.
///
/// This is a hack which stops `StoreOpener` before it attempts migration.
/// Ideally we would propagate errors nicely but that would complicate the API
/// a wee bit so instead we’re panicking.  This shouldn’t be a big deal since
/// the deprecated options are going away ‘soon’.
fn assert_no_deprecated_config(config: &crate::config::NearConfig) {
    use near_store::config::MigrationSnapshot;

    let example = match (
        config.config.use_db_migration_snapshot,
        config.config.db_migration_snapshot_path.as_ref(),
    ) {
        (None, None) => return,
        (Some(false), _) => MigrationSnapshot::Enabled(false),
        (_, None) => MigrationSnapshot::Enabled(true),
        (_, Some(path)) => MigrationSnapshot::Path(path.join("migration-snapshot")),
    };
    panic!(
        "‘use_db_migration_snapshot’ and ‘db_migration_snapshot_path’ options \
         are deprecated.\nSet ‘store.migration_snapshot’ to instead, e.g.:\n{}",
        example.format_example()
    )
}

impl<'a> near_store::StoreMigrator for Migrator<'a> {
    fn check_support(&self, version: DbVersion) -> Result<(), &'static str> {
        assert_no_deprecated_config(self.config);
        // TODO(mina86): Once open ranges in match are stabilised, get rid of
        // this constant and change the match to be 27..DB_VERSION.
        const LAST_SUPPORTED: DbVersion = DB_VERSION - 1;
        match version {
            0..=26 => Err("1.26"),
            27..=LAST_SUPPORTED => Ok(()),
            _ => unreachable!(),
        }
    }

    fn migrate(&self, storage: &NodeStorage, version: DbVersion) -> anyhow::Result<()> {
        match version {
            0..=26 => unreachable!(),
            27 => {
                // version 27 => 28: add DBCol::StateChangesForSplitStates
                //
                // Does not need to do anything since open db with option
                // `create_missing_column_families`.  Nevertheless need to bump
                // db version, because db_version 27 binary can't open
                // db_version 28 db.  Combine it with migration from 28 to 29;
                // don’t do anything here.
                Ok(())
            }
            28 => near_store::migrations::migrate_28_to_29(storage),
            29 => near_store::migrations::migrate_29_to_30(storage),
            30 => migrate_30_to_31(storage, &self.config),
            31 => near_store::migrations::migrate_31_to_32(storage),
            32 => near_store::migrations::migrate_32_to_33(storage),
            33 => {
                near_store::migrations::migrate_33_to_34(storage, self.config.client_config.archive)
            }
            #[cfg(feature = "protocol_feature_flat_state")]
            34 => migrate_34_to_35(storage, &self.config),
            DB_VERSION.. => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_mainnet_res::mainnet_restored_receipts;
    use near_mainnet_res::mainnet_storage_usage_delta;
    use near_primitives::hash::hash;

    #[test]
    fn test_migration_data() {
        assert_eq!(
            hash(serde_json::to_string(&mainnet_storage_usage_delta()).unwrap().as_bytes())
                .to_string(),
            "2fEgaLFBBJZqgLQEvHPsck4NS3sFzsgyKaMDqTw5HVvQ"
        );
        let mainnet_migration_data = load_migration_data("mainnet");
        assert_eq!(mainnet_migration_data.storage_usage_delta.len(), 3112);
        let testnet_migration_data = load_migration_data("testnet");
        assert_eq!(testnet_migration_data.storage_usage_delta.len(), 0);
    }

    #[test]
    fn test_restored_receipts_data() {
        assert_eq!(
            hash(serde_json::to_string(&mainnet_restored_receipts()).unwrap().as_bytes())
                .to_string(),
            "48ZMJukN7RzvyJSW9MJ5XmyQkQFfjy2ZxPRaDMMHqUcT"
        );
        let mainnet_migration_data = load_migration_data("mainnet");
        assert_eq!(mainnet_migration_data.restored_receipts.get(&0u64).unwrap().len(), 383);
        let testnet_migration_data = load_migration_data("testnet");
        assert!(testnet_migration_data.restored_receipts.is_empty());
    }
}
