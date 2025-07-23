#![cfg(test)]
//! Set of tests over the 'Database' interface, that we can run over multiple implementations
//! to make sure that they are working correctly.
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

use itertools::Itertools;
use near_crypto::{InMemorySigner, PublicKey};
use near_primitives::account::{AccessKey, Account, AccountContract};
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, BlockHeight, StateChangeCause};
use near_primitives::utils::get_block_shard_id;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use tempfile::Builder;

use crate::adapter::{StoreAdapter, StoreUpdateAdapter};
use crate::db::{DBTransaction, StatsValue, TestDB};
use crate::flat::{
    BlockInfo, FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata, FlatStorageStatus,
};
use crate::test_utils::TestTriesBuilder;
use crate::{
    DBCol, NodeStorage, ShardTries, StoreConfig, StoreUpdate, TrieUpdate, get_access_key,
    get_account, set_access_key, set_account,
};

/// Tests the behavior of the iterators. Iterators don't really work over cold storage, so we're not testing it here.
#[test]
fn test_db_iter() {
    let (_tmp_dir, opener) = NodeStorage::test_opener();
    let store = opener.open().unwrap().get_hot_store();
    let test_db = TestDB::new();
    for db in [&*test_db as _, store.database()] {
        let mut transaction = DBTransaction::new();
        transaction.insert(DBCol::Block, "a".into(), "val_a".into());
        transaction.insert(DBCol::Block, "aa".into(), "val_aa".into());
        transaction.insert(DBCol::Block, "aa1".into(), "val_aa1".into());
        transaction.insert(DBCol::Block, "bb1".into(), "val_bb1".into());
        transaction.insert(DBCol::Block, "cc1".into(), "val_cc1".into());
        db.write(transaction).unwrap();

        let keys: Vec<_> = db
            .iter(DBCol::Block)
            .map(|data| String::from_utf8(data.unwrap().0.to_vec()).unwrap())
            .collect();
        assert_eq!(keys, vec!["a", "aa", "aa1", "bb1", "cc1"]);

        let keys: Vec<_> = db
            .iter_range(DBCol::Block, Some("aa".as_bytes()), Some("bb1".as_bytes()))
            .map(|data| String::from_utf8(data.unwrap().0.to_vec()).unwrap())
            .collect();
        assert_eq!(keys, vec!["aa", "aa1"]);
    }
}

#[test]
fn test_print_batch() {
    let file = "/tmp/1752257522372545000-BlockMisc.batch";
    let mut file = std::fs::File::open(file).expect("Failed to open file");
    let transaction: DBTransaction =
        borsh::de::from_reader(&mut file).expect("Failed to deserialize transaction");
    eprintln!("Transaction: {:#?}", transaction);
}

// cspell:words tikv jemallocator Jemalloc
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[test]
fn test_replay_batches() {
    // Get environment variable BATCHES_DIR
    let Ok(batches_dir) = std::env::var("BATCHES_DIR") else {
        // skip the test if the environment variable is not set
        println!("Skipping test_replay_batches: BATCHES_DIR not set");
        return;
    };

    let home = env::var("HOME").expect("HOME not set");
    let base_dir = PathBuf::from(home).join("tmp-db");
    std::fs::create_dir_all(&base_dir).unwrap();
    let tmp_dir = Builder::new().prefix("mytemp-").tempdir_in(&base_dir).unwrap();

    // let tmp_dir = tempfile::tempdir().unwrap();

    // Use default StoreConfig rather than NodeStorage::test_opener so we’re using the
    // same configuration as in production.
    let mut store_config = StoreConfig::default();
    store_config.enable_statistics = true;
    let mut store =
        NodeStorage::opener(tmp_dir.path(), &store_config, None).open().unwrap().get_hot_store();
    store.set_output_batches(false);

    // Iterate over all files in the directory, alphabetically
    let mut entries: Vec<_> = std::fs::read_dir(batches_dir)
        .expect("Failed to read directory")
        .filter_map(Result::ok)
        .collect();
    entries.sort_by_key(|entry| entry.path());

    let mut nanos_last: Option<u64> = None;
    let mut last_block_processed_at: Option<std::time::Instant> = None;
    let mut total_blocking_time = std::time::Duration::ZERO;

    for entry in entries {
        // Parse the filename as {nanos}-{string}.batch
        let filename = entry.file_name();
        let filename_str = filename.to_string_lossy();
        let Some((nanos, cols)) = filename_str.split_once('-') else {
            // skip files that do not match the expected format
            println!("Skipping file: {} (does not match expected format)", filename_str);
            continue;
        };

        let path = entry.path();
        // Check if the file is a regular file
        if !path.is_file() {
            continue;
        }

        let file_len = entry.metadata().expect("Failed to get metadata").len();
        let mut reader = std::fs::File::open(&path).expect("Failed to open file");
        let transaction: DBTransaction = match borsh::de::from_reader(&mut reader) {
            Ok(tx) => tx,
            Err(e) => {
                println!("Failed to deserialize transaction from {}: {}", filename_str, e);
                continue;
            }
        };

        // Critical path for postprocess_ready_block goes like:
        // Block-BlockHeader-...-State-... etc, (main chain update)
        // Flat-FlatState-... etc, (flat state update)
        // Try to wait as long as it took between blocks during actual processing,
        // to allow rocksdb background work to be more realistic.
        if cols.starts_with("Block-") {
            let nanos = nanos.parse::<u64>().expect("Failed to parse nanos");
            match (nanos_last, last_block_processed_at) {
                (Some(last), Some(processed_at)) => {
                    let elapsed =
                        std::time::Instant::now().duration_since(processed_at).as_nanos() as u64;
                    if elapsed < nanos - last {
                        let delay = std::time::Duration::from_nanos(std::cmp::min(
                            100_000_000, // at most 100 ms delay
                            nanos - last - elapsed,
                        ));
                        eprintln!(
                            "Waiting for {} milliseconds (raw delta {})  before processing {}",
                            delay.as_millis(),
                            (nanos - last - elapsed) / 1_000_000,
                            filename_str
                        );
                        std::thread::sleep(delay);
                    }
                }
                (_, _) => {
                    eprintln!("Processing first file: {} at {} nanoseconds", filename_str, nanos);
                }
            };
            nanos_last = Some(nanos);
            last_block_processed_at = Some(std::time::Instant::now());
        }

        let tx_ops_len = transaction.ops.len();
        if cols.starts_with("Block-") || cols.starts_with("FlatState-FlatStateChanges-") {
            //print_batch_stats(cols, &transaction);
        }
        let now = std::time::Instant::now();
        store.write(transaction).expect("Failed to write transaction to store");
        if cols.starts_with("Block-") || cols.starts_with("FlatState-FlatStateChanges-") {
            //print_batch_stats(cols, &transaction);
        } else {
            store.database().flush_wal().expect("Failed to flush WAL");
        }
        let elapsed = now.elapsed();

        if cols.starts_with("Block-") || cols.starts_with("FlatState-FlatStateChanges-") {
            total_blocking_time += elapsed;
        }

        if elapsed > std::time::Duration::from_millis(1) {
            println!(
                "Processing file: {}\t(total blocking: {}ms)\t{} items\t{} KBs\telapsed: {:?}ms\tcols: {}",
                nanos,
                total_blocking_time.as_millis(),
                tx_ops_len,
                file_len / 1024,
                elapsed.as_millis(),
                cols
            );
        }
    }
}

fn print_batch_stats(cols: &str, transaction: &DBTransaction) {
    let mut col_ops: HashMap<&str, usize> = HashMap::new();
    let mut col_size: HashMap<&str, usize> = HashMap::new();
    for op in &transaction.ops {
        let name: &'static str = op.col().into();
        *col_ops.entry(name).or_default() += 1;
        *col_size.entry(name).or_default() += op.bytes();
    }
    let col_ops_str = col_ops.iter().map(|(col, count)| format!("{}: {}", col, count)).join(", ");
    let col_size_str =
        col_size.iter().map(|(col, size)| format!("{}: {} bytes", col, size)).join(", ");
    eprintln!("Transaction: {} cols: [{}] ops: [{}]", cols, col_ops_str, col_size_str);
}

#[test]
fn test_stress_trie_and_storage() {
    let db_dir = env::var("DB_DIR").expect("DB_DIR not set");
    std::fs::create_dir_all(&db_dir).unwrap();
    let db_path = PathBuf::from(db_dir);

    let desired_accounts = env::var("DESIRED_ACCOUNTS")
        .unwrap_or_else(|_| "50000".to_string())
        .parse::<usize>()
        .expect("Failed to parse DESIRED_ACCOUNTS");

    let updates_batch_size = env::var("UPDATES_BATCH_SIZE")
        .unwrap_or_else(|_| "20000".to_string())
        .parse::<usize>()
        .expect("Failed to parse UPDATES_BATCH_SIZE");

    let update_batches = env::var("UPDATE_BATCHES")
        .unwrap_or_else(|_| "100".to_string())
        .parse::<usize>()
        .expect("Failed to parse UPDATE_BATCHES");

    let skip_deletions = env::var("SKIP_DELETIONS")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .expect("Failed to parse SKIP_DELETIONS");

    // Use default StoreConfig rather than NodeStorage::test_opener so we’re using the
    // same configuration as in production.
    let mut store = NodeStorage::opener(db_path.as_path(), &Default::default(), None)
        .open()
        .unwrap()
        .get_hot_store();
    store.set_output_batches(false);

    let key = "num_accounts".as_bytes();
    let num_existing_accounts = store.get_ser::<u64>(DBCol::Misc, key).unwrap_or(None).unwrap_or(0);

    let shard_layout = ShardLayout::single_shard();
    let shard_uids = shard_layout.shard_uids().collect_vec();
    let shard_uid = shard_uids[0];

    let tries = TestTriesBuilder::new()
        .with_store(store)
        .with_shard_layout(shard_layout)
        .with_flat_storage(true)
        .with_in_memory_tries(true)
        .build();

    // Get the starting point:
    let FlatStorageStatus::Ready(status) =
        tries.get_flat_storage_manager().get_flat_storage_status(shard_uid)
    else {
        panic!("Flat storage is not ready for shard {}", shard_uid);
    };
    let mut block_height = status.flat_head.height;
    let mut state_root = status.flat_head.hash;

    eprintln!(
        "Starting with: height: {}, state root: {}, existing accounts: {}",
        block_height, state_root, num_existing_accounts
    );

    let accounts_per_batch = 10_000;
    let batches = desired_accounts / accounts_per_batch;
    let log_each = 10;
    let mut measured_times = MeasuredTimes::new();
    let mut account_infos = Vec::with_capacity(batches * accounts_per_batch);

    let start_idx = num_existing_accounts as usize / accounts_per_batch as usize;
    for i in 0..start_idx {
        // Just recreate these instead of reading them from store.
        let accounts = make_new_accounts(i * accounts_per_batch, accounts_per_batch);
        account_infos.extend(accounts.iter().map(|(id, _, pub_key)| (id.clone(), pub_key.clone())));
    }

    for i in start_idx..batches {
        let accounts = make_new_accounts(i * accounts_per_batch, accounts_per_batch);
        account_infos.extend(accounts.iter().map(|(id, _, pub_key)| (id.clone(), pub_key.clone())));

        let now = std::time::Instant::now();
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        assert!(trie.has_memtries());
        let mut trie_update = TrieUpdate::new(trie);
        for (account_id, account, public_key) in accounts {
            set_account(&mut trie_update, account_id.clone(), &account);
            set_access_key(&mut trie_update, account_id, public_key, &AccessKey::full_access());
        }
        trie_update.commit(StateChangeCause::InitialState);
        let update_result = trie_update.finalize().unwrap();
        let (trie_changes, state_changes) =
            (update_result.trie_changes, update_result.state_changes);

        let mut store_update = tries.store_update();

        let new_root = trie_changes.new_root;
        tries.apply_insertions(&trie_changes, shard_uid, &mut store_update);
        if !skip_deletions {
            tries.apply_deletions(&trie_changes, shard_uid, &mut store_update);
        }

        let memtrie_root =
            tries.apply_memtrie_changes(&trie_changes, shard_uid, block_height).unwrap();
        assert_eq!(new_root, memtrie_root);

        if block_height > 0 {
            tries.delete_memtrie_roots_up_to_height(shard_uid, block_height - 1);
        }
        flat_storage_update(
            &tries,
            shard_uid,
            state_root,
            new_root,
            block_height + 1,
            store_update.store_update(),
            FlatStateChanges::from_state_changes(&state_changes),
        );
        measured_times.add_memory(now.elapsed());

        let now = std::time::Instant::now();
        let num_accounts = (i + 1) * accounts_per_batch;
        store_update.store_update().set_ser(DBCol::Misc, key, &num_accounts).unwrap();
        store_update.commit().unwrap();
        measured_times.add_disk(now.elapsed());

        let now = std::time::Instant::now();
        let flat = tries.get_flat_storage_manager().get_flat_storage_for_shard(shard_uid).unwrap();
        flat.update_flat_head(&new_root).unwrap();
        measured_times.add_flat(now.elapsed());

        if (i + 1) % log_each == 0 {
            eprintln!(
                "Processed batch {:04}: {:08} accounts, new root: {}, (memory: {:?}, disk: {:?}, flat: {:?})",
                i + 1,
                (i + 1) * accounts_per_batch,
                new_root,
                measured_times.memory.as_millis() / (log_each as u128),
                measured_times.disk.as_millis() / (log_each as u128),
                measured_times.flat.as_millis() / (log_each as u128)
            );
            measured_times = MeasuredTimes::new();
        }

        state_root = new_root;
        block_height += 1;
    }

    let batches = update_batches;
    let updates_per_batch = updates_batch_size;

    // Seed the random number generator
    let mut rng = StdRng::seed_from_u64(0);

    for i in 0..batches {
        let now = std::time::Instant::now();
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        assert!(trie.has_memtries());
        let trie = trie.recording_reads_new_recorder();
        let mut trie_update = TrieUpdate::new(trie);
        for _ in 0..updates_per_batch {
            // Choose 2 random accounts to update
            let (from_id, from_key) = account_infos.choose(&mut rng).unwrap();
            let (to_id, _to_key) = account_infos.choose(&mut rng).unwrap();

            let mut from_account = get_account(&trie_update, from_id).unwrap().unwrap();
            let mut to_account = get_account(&trie_update, to_id).unwrap().unwrap();

            // Transfer half of the amount from one account to another
            let transfer_amount = from_account.amount() / 2;
            from_account.set_amount(from_account.amount() - transfer_amount);
            to_account.set_amount(to_account.amount() + transfer_amount);

            set_account(&mut trie_update, from_id.clone(), &from_account);
            set_account(&mut trie_update, to_id.clone(), &to_account);

            let mut from_access_key =
                get_access_key(&trie_update, from_id, from_key).unwrap().unwrap();
            from_access_key.nonce += 1;
            set_access_key(&mut trie_update, from_id.clone(), from_key.clone(), &from_access_key);
        }
        trie_update.commit(StateChangeCause::InitialState);
        let update_result = trie_update.finalize().unwrap();
        let (trie_changes, state_changes) =
            (update_result.trie_changes, update_result.state_changes);

        let mut store_update = tries.store_update();

        let new_root = trie_changes.new_root;
        tries.apply_insertions(&trie_changes, shard_uid, &mut store_update);
        if !skip_deletions {
            tries.apply_deletions(&trie_changes, shard_uid, &mut store_update);
        }

        let memtrie_root =
            tries.apply_memtrie_changes(&trie_changes, shard_uid, block_height).unwrap();
        assert_eq!(new_root, memtrie_root);

        if block_height > 0 {
            tries.delete_memtrie_roots_up_to_height(shard_uid, block_height - 1);
        }

        flat_storage_update(
            &tries,
            shard_uid,
            state_root,
            new_root,
            block_height + 1,
            store_update.store_update(),
            FlatStateChanges::from_state_changes(&state_changes),
        );
        measured_times.add_memory(now.elapsed());

        let now = std::time::Instant::now();
        store_update.commit().unwrap();
        measured_times.add_disk(now.elapsed());

        let now = std::time::Instant::now();
        let flat = tries.get_flat_storage_manager().get_flat_storage_for_shard(shard_uid).unwrap();
        flat.update_flat_head(&new_root).unwrap();
        measured_times.add_flat(now.elapsed());

        if (i + 1) % log_each == 0 {
            tries
                .store()
                .store_ref()
                .database()
                .get_store_statistics()
                .unwrap()
                .data
                .iter()
                .find(|(key, _)| key == "rocksdb.estimate-live-data-size")
                .map(|(_, stats)| {
                    for i in stats {
                        if let StatsValue::ColumnValue(col, val) = i {
                            if col == &DBCol::State
                                || col == &DBCol::FlatStateChanges
                                || col == &DBCol::FlatState
                            {
                                eprint!("{}: {}\t", col, val / 1024 / 1024);
                            }
                        }
                    }
                    eprintln!("");
                });
            eprintln!(
                "Processed batch (updates) {:04}:  new root: {}, (memory: {:?}, disk: {:?}, flat: {:?})",
                i + 1,
                new_root,
                measured_times.memory.as_millis() / (log_each as u128),
                measured_times.disk.as_millis() / (log_each as u128),
                measured_times.flat.as_millis() / (log_each as u128)
            );
            measured_times = MeasuredTimes::new();
        }

        state_root = new_root;
        block_height += 1;
    }
}

fn flat_storage_update(
    tries: &ShardTries,
    shard_uid: ShardUId,
    prev_state_root: CryptoHash,
    new_state_root: CryptoHash,
    new_height: BlockHeight,
    store_update: &mut StoreUpdate,
    flat_state_changes: FlatStateChanges,
) {
    let delta = FlatStateDelta {
        changes: flat_state_changes,
        metadata: FlatStateDeltaMetadata {
            block: BlockInfo {
                hash: new_state_root,
                height: new_height,
                prev_hash: prev_state_root,
            },
            prev_block_with_changes: None,
        },
    };
    let flat_storage =
        tries.get_flat_storage_manager().get_flat_storage_for_shard(shard_uid).unwrap();

    let new_store_update = flat_storage.add_delta(delta).unwrap();
    store_update.merge(new_store_update.into());

    let chunk_extra = ChunkExtra::new(
        &new_state_root,
        CryptoHash::default(),
        vec![],
        0,
        0,
        0,
        Some(CongestionInfo::default()),
        BandwidthRequests::empty(),
    );
    store_update
        .set_ser(
            DBCol::ChunkExtra,
            &get_block_shard_id(&new_state_root, shard_uid.shard_id()),
            &chunk_extra,
        )
        .unwrap();
}

struct MeasuredTimes {
    memory: std::time::Duration,
    disk: std::time::Duration,
    flat: std::time::Duration,
}

impl MeasuredTimes {
    fn new() -> Self {
        Self {
            memory: std::time::Duration::ZERO,
            disk: std::time::Duration::ZERO,
            flat: std::time::Duration::ZERO,
        }
    }

    fn add_memory(&mut self, duration: std::time::Duration) {
        self.memory += duration;
    }

    fn add_disk(&mut self, duration: std::time::Duration) {
        self.disk += duration;
    }

    fn add_flat(&mut self, duration: std::time::Duration) {
        self.flat += duration;
    }
}

fn fake_hash(height: usize) -> CryptoHash {
    hash(height.to_le_bytes().as_ref())
}

fn make_new_accounts(start_idx: usize, count: usize) -> Vec<(AccountId, Account, PublicKey)> {
    (start_idx..start_idx + count)
        .map(|i| {
            let account_id =
                format!("test_account_{}", fake_hash(i)).to_ascii_lowercase().parse().unwrap();
            let amount = 1000 + i as u128;
            let locked = 0;
            let account = Account::new(amount, locked, AccountContract::None, 0);
            let signer = InMemorySigner::test_signer(&account_id);
            (account_id, account, signer.public_key())
        })
        .collect()
}
