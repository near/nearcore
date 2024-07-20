use std::fs;
use std::path::Path;

use anyhow::anyhow;
use borsh::BorshDeserialize;
use indicatif::ParallelProgressIterator;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisConfig;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::hash::CryptoHash;
use near_primitives::state::FlatStateValue;
use near_store::flat::delta::KeyForFlatStateDelta;
use near_store::flat::store_helper::{
    decode_flat_state_db_key, encode_flat_state_db_key, get_flat_storage_status,
};
use near_store::flat::FlatStateChanges;
use near_store::trie::mem::loading::get_state_root;
use near_store::trie::mem::parallel_loader::{
    calculate_end_key, make_memtrie_parallel_loading_plan,
};
use near_store::{DBCol, NodeStorage, Store};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use strum::IntoEnumIterator;

pub(crate) fn open_rocksdb(
    home: &Path,
    mode: near_store::Mode,
) -> anyhow::Result<near_store::db::RocksDB> {
    let config = nearcore::config::Config::from_file_skip_validation(
        &home.join(nearcore::config::CONFIG_FILENAME),
    )?;
    let store_config = &config.store;
    let db_path = store_config.path.as_ref().cloned().unwrap_or_else(|| home.join("data"));
    let rocksdb =
        near_store::db::RocksDB::open(&db_path, store_config, mode, near_store::Temperature::Hot)?;
    Ok(rocksdb)
}

pub(crate) fn open_state_snapshot(home: &Path, mode: near_store::Mode) -> anyhow::Result<Store> {
    let config = nearcore::config::Config::from_file_skip_validation(
        &home.join(nearcore::config::CONFIG_FILENAME),
    )?;
    let store_config = &config.store;
    let db_path = store_config.path.as_ref().cloned().unwrap_or_else(|| home.join("data"));

    let state_snapshot_dir = db_path.join("state_snapshot");
    let snapshots: Result<Vec<_>, _> = fs::read_dir(state_snapshot_dir)?.into_iter().collect();
    let snapshots = snapshots?;
    let &[snapshot_dir] = &snapshots.as_slice() else {
        return Err(anyhow!("found more than one snapshot"));
    };

    let path = snapshot_dir.path();
    println!("state snapshot path {path:?}");

    let opener = NodeStorage::opener(&path, false, &store_config, None);
    let storage = opener.open_in_mode(mode)?;
    let store = storage.get_hot_store();

    Ok(store)
}

pub(crate) fn resolve_column(col_name: &str) -> anyhow::Result<DBCol> {
    DBCol::iter()
        .filter(|db_col| <&str>::from(db_col) == col_name)
        .next()
        .ok_or_else(|| anyhow!("column {col_name} does not exist"))
}

pub struct MemtrieStateTrimmingCalculationResult {
    /// State column entries that we need to keep in order to load memtries.
    pub state_entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// A reasonable partitioning of the FlatState column keys. It is guaranteed
    /// that these are non-overlapping, and cover the entire range of keys corresponding
    /// to all the current shards.
    ///
    /// This is used to perform a parallel copy of FlatState to a fresh database.
    pub flat_state_ranges: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Reads all data from the State column that is necessary to load memtrie based on flat storage,
/// for all shards of the current sharding version. These include the values that are not inlined,
/// as well as some top trie nodes that are used during parallel memtrie loading. See the top of
/// parallel_loader module for a description of the parallel loading algorithm and why these nodes
/// are needed.
///
/// Also return a reasonable partitioning of the FlatState column keys.
///
/// Despite returning Result, for readability and ease of implementation, this function will panic
/// for some errors that are not meaningful to recover from anyway.
pub fn prepare_memtrie_state_trimming(
    store: Store,
    genesis_config: &GenesisConfig,
    include_flat_delta: bool,
) -> anyhow::Result<MemtrieStateTrimmingCalculationResult> {
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), genesis_config);
    let chain = ChainStore::new(store.clone(), genesis_config.genesis_height, false);
    let final_head = chain.final_head()?;
    let epoch_id = final_head.epoch_id;
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id)?;
    let shard_uids = shard_layout.shard_uids().collect::<Vec<_>>();

    let mut flat_state_ranges = Vec::new();

    // Spawn a thread to collect all the important keys that we'll discover.
    let (important_keys_tx, important_keys_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(1000000);
    let important_keys_collect_thread = std::thread::spawn(move || {
        let mut important_keys = Vec::new();
        while let Ok(key) = important_keys_rx.recv() {
            important_keys.push(key);
        }
        important_keys
    });
    for shard_uid in shard_uids {
        tracing::info!("Reading shard {shard_uid}...");
        let flat_status = get_flat_storage_status(&store, shard_uid)?;
        let flat_head = match flat_status {
            near_store::flat::FlatStorageStatus::Ready(ready) => ready.flat_head.hash,
            _ => panic!("Flat storage is not ready for shard {shard_uid}"),
        };
        let root = get_state_root(&store, flat_head, shard_uid)?;
        if root == CryptoHash::default() {
            tracing::info!("Shard {shard_uid} has empty state; skipping.");
            continue;
        }
        let plan = make_memtrie_parallel_loading_plan(store.clone(), shard_uid, root)?;

        // The nodes that are needed to load the memtrie must be kept in the State column.
        for hash in plan.node_hashes() {
            let key = encode_flat_state_db_key(shard_uid, &hash.0);
            important_keys_tx.send(key).unwrap();
        }

        // Memtrie returns us a list of prefixes that can be used to load the memtrie in parallel.
        // However, since these ranges exclude the leaf nodes that are in the plan, we need to tweak
        // the ranges a bit to make sure they cover all the FlatState keys. We'll do that by simply
        // taking into account only the start key of each prefix (ignoring the first).
        let mut split_points = plan
            .subtrees_to_load
            .into_iter()
            .map(|subtree| {
                let (start, _) = subtree.to_iter_range(shard_uid);
                start
            })
            .collect::<Vec<_>>();
        split_points.sort();
        split_points.remove(0); // skip first split point

        // Make a list of ranges, [shard start, key1), [key1, key2), ..., [key_n, shard end).
        let shard_uid_start_key = shard_uid.to_bytes().to_vec();
        let shard_uid_end_key = calculate_end_key(&shard_uid_start_key, 1).unwrap();
        let mut ranges = Vec::new();
        for i in 0..=split_points.len() {
            let start =
                if i == 0 { shard_uid_start_key.clone() } else { split_points[i - 1].clone() };
            let end = if i < split_points.len() {
                split_points[i].clone()
            } else {
                shard_uid_end_key.clone()
            };
            ranges.push((start, end));
        }
        // just to sanity check we covered everything.
        for i in 0..ranges.len() - 1 {
            assert_eq!(ranges[i].1, ranges[i + 1].0);
        }
        assert_eq!(&ranges[0].0, &shard_uid_start_key);
        assert_eq!(&ranges.last().unwrap().1, &shard_uid_end_key);

        flat_state_ranges.extend(ranges.clone());

        // Iterate through FlatState in parallel, and collect all the non-inlined keys.
        ranges.par_iter().progress_count(ranges.len() as u64).for_each(|(start, end)| {
            for item in store.iter_range(DBCol::FlatState, Some(start), Some(end)) {
                let (key, value) = item.unwrap();
                let value = FlatStateValue::try_from_slice(&value).unwrap();
                match value {
                    FlatStateValue::Ref(r) => {
                        let (shard_uid, _) = decode_flat_state_db_key(&key).unwrap();
                        important_keys_tx
                            .send(encode_flat_state_db_key(shard_uid, &r.hash.0))
                            .unwrap();
                    }
                    FlatStateValue::Inlined(_) => {}
                }
            }
        });
    }

    // If flat delta is desired, also read all the non-inlined keys from FlatStateChanges.
    if include_flat_delta {
        let shard_version_start_key = shard_layout.version().to_le_bytes().to_vec();
        let shard_version_end_key = calculate_end_key(&shard_version_start_key, 1).unwrap();
        for item in store.iter_range(
            DBCol::FlatStateChanges,
            Some(&shard_version_start_key),
            Some(&shard_version_end_key),
        ) {
            let (key, value) = item.unwrap();
            let changes = FlatStateChanges::try_from_slice(&value).unwrap();
            let key = KeyForFlatStateDelta::try_from_slice(&key).unwrap();
            for (_, value) in changes.0 {
                if let Some(FlatStateValue::Ref(r)) = value {
                    important_keys_tx
                        .send(encode_flat_state_db_key(key.shard_uid, &r.hash.0))
                        .unwrap();
                }
            }
        }
    }

    drop(important_keys_tx);
    let mut important_keys = important_keys_collect_thread.join().unwrap();
    important_keys.sort();
    important_keys.dedup();
    tracing::info!(
        "Found {} important keys from FlatState and FlatStateChanges to read from State column",
        important_keys.len(),
    );

    // Read the important values from the State column.
    let num_important_keys = important_keys.len();
    let important_entries = important_keys
        .into_par_iter()
        .progress_count(num_important_keys as u64)
        .map(|key| {
            let value = store
                .get_raw_bytes(DBCol::State, &key)
                .unwrap_or_else(|e| panic!("Error reading key {key:?} from State column: {e}"))
                .unwrap_or_else(|| panic!("Key {key:?} not found in State column"));
            (key, value.to_vec())
        })
        .collect::<Vec<_>>();

    Ok(MemtrieStateTrimmingCalculationResult {
        state_entries: important_entries,
        flat_state_ranges,
    })
}

/// Write the given entries to the State column, in parallel, while reporting progress.
pub fn write_state_column(store: &Store, data: Vec<(Vec<u8>, Vec<u8>)>) -> anyhow::Result<()> {
    data.par_iter()
        .progress_count(data.len() as u64)
        .try_fold(
            || (0usize, store.store_update()),
            |(mut bytes_written, mut update), (key, value)| -> anyhow::Result<_> {
                update.set_raw_bytes(DBCol::State, key, value);
                bytes_written += key.len() + value.len();
                if bytes_written > 100 * 1024 * 1024 {
                    update.commit()?;
                    Ok((0, store.store_update()))
                } else {
                    Ok((bytes_written, update))
                }
            },
        )
        .try_for_each(|update| -> anyhow::Result<()> { Ok(update?.1.commit()?) })
}
