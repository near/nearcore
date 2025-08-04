#![cfg(test)]
//! Set of tests over the 'Database' interface, that we can run over multiple implementations
//! to make sure that they are working correctly.
use std::env;
use std::path::PathBuf;

use itertools::Itertools;
use near_crypto::{InMemorySigner, PublicKey};
use near_primitives::account::{AccessKey, Account, AccountContract};
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::hash::{CryptoHash, HASH_COUNTER, HASH_MEASURE_PER, HASH_NANOS, hash};
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, BlockHeight, StateChangeCause};
use near_primitives::utils::get_block_shard_id;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

use crate::adapter::StoreUpdateAdapter;
use crate::db::{DBTransaction, TestDB};
use crate::flat::{
    BlockInfo, FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata, FlatStorageStatus,
};
use crate::test_utils::TestTriesBuilder;
use crate::{
    DBCol, NodeStorage, ShardTries, StoreUpdate, TrieUpdate, get_access_key, get_access_key_pure,
    get_account, get_account_pure, set_access_key, set_account,
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

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[test]
fn test_stress_trie_and_storage() {
    // If provided, will use the DB_DIR environment variable to store the database.
    // This can be used to populate the database with a large number of accounts.
    let db_path = if let Ok(db_dir) = env::var("DB_DIR") {
        std::fs::create_dir_all(&db_dir).unwrap();
        PathBuf::from(db_dir)
    } else {
        tempfile::tempdir().unwrap().into_path()
    };

    // Should be multiple of 10_000
    let desired_accounts = env::var("DESIRED_ACCOUNTS")
        .unwrap_or_else(|_| "50000".to_string())
        .parse::<usize>()
        .expect("Failed to parse DESIRED_ACCOUNTS");

    // Size of the "Updates" batch.
    let updates_per_batch = env::var("UPDATES_PER_BATCH")
        .unwrap_or_else(|_| "100000".to_string())
        .parse::<usize>()
        .expect("Failed to parse UPDATES_PER_BATCH");

    // Set to "false" to just measure memtries, not actually persist updates.
    let persist_updates = env::var("PERSIST_UPDATES")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()
        .expect("Failed to parse PERSIST_UPDATES");

    // Set to "false" to bypass recording reads in the trie.
    let with_recording = env::var("WITH_RECORDING")
        .unwrap_or_else(|_| "true".to_string())
        .parse::<bool>()
        .expect("Failed to parse WITH_RECORDING");

    let store = NodeStorage::opener(db_path.as_path(), &Default::default(), None, None)
        .open()
        .unwrap()
        .get_hot_store();

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

    // Phase 1: Populate the database with accounts.
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
        let new_root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
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

    let batches = 100;

    // Seed the random number generator
    let mut rng = StdRng::seed_from_u64(0);
    let mut hash_counter = 0;
    let mut hash_nanos = 0;

    let get_account = if with_recording { get_account } else { get_account_pure };
    let get_access_key = if with_recording { get_access_key } else { get_access_key_pure };

    // Phase 2: Update accounts in batches.
    for i in 0..batches {
        let now = std::time::Instant::now();
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        assert!(trie.has_memtries());
        let trie = if with_recording { trie.recording_reads_new_recorder() } else { trie };
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
        let new_root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
        let memtrie_root =
            tries.apply_memtrie_changes(&trie_changes, shard_uid, block_height).unwrap();
        assert_eq!(new_root, memtrie_root);

        if block_height > 0 {
            tries.delete_memtrie_roots_up_to_height(shard_uid, block_height - 1);
        }

        if persist_updates {
            flat_storage_update(
                &tries,
                shard_uid,
                state_root,
                new_root,
                block_height + 1,
                store_update.store_update(),
                FlatStateChanges::from_state_changes(&state_changes),
            );
        }
        measured_times.add_memory(now.elapsed());

        let now = std::time::Instant::now();
        if persist_updates {
            store_update.commit().unwrap();
        }
        measured_times.add_disk(now.elapsed());

        let now = std::time::Instant::now();
        if persist_updates {
            let flat =
                tries.get_flat_storage_manager().get_flat_storage_for_shard(shard_uid).unwrap();
            flat.update_flat_head(&new_root).unwrap();
        }
        measured_times.add_flat(now.elapsed());

        if (i + 1) % log_each == 0 {
            let next_hash_counter = HASH_COUNTER.load(std::sync::atomic::Ordering::Relaxed);
            let next_hash_nanos = HASH_NANOS.load(std::sync::atomic::Ordering::Relaxed) as u128;
            eprintln!(
                "Processed batch (updates) {:04}:  new root: {}, (memory: {:?}, disk: {:?}, flat: {:?}, hashes: {:?}, hashes time: {:?})",
                i + 1,
                new_root,
                measured_times.memory.as_millis() / (log_each as u128),
                measured_times.disk.as_millis() / (log_each as u128),
                measured_times.flat.as_millis() / (log_each as u128),
                (next_hash_counter - hash_counter) / (log_each as usize),
                (next_hash_nanos - hash_nanos) * HASH_MEASURE_PER as u128
                    / (1_000_000 as u128)
                    / (log_each as u128)
            );
            measured_times = MeasuredTimes::new();
            hash_counter = next_hash_counter;
            hash_nanos = next_hash_nanos;
        }

        state_root = new_root;
        block_height += 1;
        std::thread::sleep(std::time::Duration::from_millis(100)); // sleep to see batches in profiler
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
