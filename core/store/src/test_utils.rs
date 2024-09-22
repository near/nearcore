use crate::adapter::{StoreAdapter, StoreUpdateAdapter, StoreUpdateCommit};
use crate::db::TestDB;
use crate::flat::{BlockInfo, FlatStorageManager, FlatStorageReadyStatus, FlatStorageStatus};
use crate::metadata::{DbKind, DbVersion, DB_VERSION};
use crate::{
    get, get_delayed_receipt_indices, get_promise_yield_indices, DBCol, NodeStorage, ShardTries,
    StateSnapshotConfig, Store, Trie, TrieConfig,
};
use itertools::Itertools;
use near_primitives::account::id::AccountId;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{DataReceipt, PromiseYieldTimeout, Receipt, ReceiptEnum, ReceiptV1};
use near_primitives::shard_layout::{get_block_shard_uid, ShardUId, ShardVersion};
use near_primitives::state::FlatStateValue;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{NumShards, StateRoot};
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};
use rand::seq::SliceRandom;
use rand::Rng;
use std::collections::HashMap;
use std::str::{from_utf8, FromStr};
use std::sync::Arc;

/// Creates an in-memory node storage.
///
/// In tests you’ll often want to use [`create_test_store`] instead.
pub fn create_test_node_storage(version: DbVersion, hot_kind: DbKind) -> NodeStorage {
    let storage = NodeStorage::new(TestDB::new());

    storage.get_hot_store().set_db_version(version).unwrap();
    storage.get_hot_store().set_db_kind(hot_kind).unwrap();

    storage
}

/// Creates an in-memory node storage.
///
/// In tests you’ll often want to use [`create_test_store`] or
/// [`create_test_split_store`] (for archival nodes) instead.
/// It initializes the db version and db kind to sensible defaults -
/// the current version and rpc kind.
pub fn create_test_node_storage_default() -> NodeStorage {
    create_test_node_storage(DB_VERSION, DbKind::RPC)
}

/// Creates an in-memory node storage with ColdDB
pub fn create_test_node_storage_with_cold(
    version: DbVersion,
    hot_kind: DbKind,
) -> (NodeStorage, Arc<TestDB>, Arc<TestDB>) {
    let hot = TestDB::new();
    let cold = TestDB::new();
    let storage = NodeStorage::new_with_cold(hot.clone(), cold.clone());

    storage.get_hot_store().set_db_version(version).unwrap();
    storage.get_hot_store().set_db_kind(hot_kind).unwrap();
    storage.get_cold_store().unwrap().set_db_version(version).unwrap();
    storage.get_cold_store().unwrap().set_db_kind(DbKind::Cold).unwrap();

    (storage, hot, cold)
}

/// Creates an in-memory database.
pub fn create_test_store() -> Store {
    create_test_node_storage(DB_VERSION, DbKind::RPC).get_hot_store()
}

/// Returns a pair of (Hot, Split) store to be used for setting up archival clients.
/// Note that the Split store contains both Hot and Cold stores.
pub fn create_test_split_store() -> (Store, Store) {
    let (storage, ..) = create_test_node_storage_with_cold(DB_VERSION, DbKind::Hot);
    (storage.get_hot_store(), storage.get_split_store().unwrap())
}

pub struct TestTriesBuilder {
    store: Option<Store>,
    shard_version: ShardVersion,
    num_shards: NumShards,
    enable_flat_storage: bool,
    enable_in_memory_tries: bool,
}

impl TestTriesBuilder {
    pub fn new() -> Self {
        Self {
            store: None,
            shard_version: 0,
            num_shards: 1,
            enable_flat_storage: false,
            enable_in_memory_tries: false,
        }
    }

    pub fn with_store(mut self, store: Store) -> Self {
        self.store = Some(store);
        self
    }

    pub fn with_shard_layout(mut self, shard_version: ShardVersion, num_shards: NumShards) -> Self {
        self.shard_version = shard_version;
        self.num_shards = num_shards;
        self
    }

    pub fn with_flat_storage(mut self, enable: bool) -> Self {
        self.enable_flat_storage = enable;
        self
    }

    pub fn with_in_memory_tries(mut self, enable: bool) -> Self {
        self.enable_in_memory_tries = enable;
        self
    }

    pub fn build(self) -> ShardTries {
        if self.enable_in_memory_tries && !self.enable_flat_storage {
            panic!("In-memory tries require flat storage");
        }
        let store = self.store.unwrap_or_else(create_test_store);
        let shard_uids = (0..self.num_shards)
            .map(|shard_id| ShardUId { shard_id: shard_id as u32, version: self.shard_version })
            .collect::<Vec<_>>();
        let flat_storage_manager = FlatStorageManager::new(store.flat_store());
        let tries = ShardTries::new(
            store.clone(),
            TrieConfig {
                load_mem_tries_for_tracked_shards: self.enable_in_memory_tries,
                ..Default::default()
            },
            &shard_uids,
            flat_storage_manager,
            StateSnapshotConfig::default(),
        );
        if self.enable_flat_storage {
            let mut store_update = tries.store_update();
            for shard_id in 0..self.num_shards {
                let shard_uid = ShardUId {
                    version: self.shard_version,
                    shard_id: shard_id.try_into().unwrap(),
                };
                store_update.flat_store_update().set_flat_storage_status(
                    shard_uid,
                    FlatStorageStatus::Ready(FlatStorageReadyStatus {
                        flat_head: BlockInfo::genesis(CryptoHash::default(), 0),
                    }),
                );
            }
            store_update.commit().unwrap();

            let flat_storage_manager = tries.get_flat_storage_manager();
            for shard_id in 0..self.num_shards {
                let shard_uid = ShardUId {
                    version: self.shard_version,
                    shard_id: shard_id.try_into().unwrap(),
                };
                flat_storage_manager.create_flat_storage_for_shard(shard_uid).unwrap();
            }
        }
        if self.enable_in_memory_tries {
            // ChunkExtra is needed for in-memory trie loading code to query state roots.
            let congestion_info = ProtocolFeature::CongestionControl
                .enabled(PROTOCOL_VERSION)
                .then(CongestionInfo::default);
            let chunk_extra = ChunkExtra::new(
                PROTOCOL_VERSION,
                &Trie::EMPTY_ROOT,
                CryptoHash::default(),
                Vec::new(),
                0,
                0,
                0,
                congestion_info,
            );
            let mut update_for_chunk_extra = store.store_update();
            for shard_uid in &shard_uids {
                update_for_chunk_extra
                    .set_ser(
                        DBCol::ChunkExtra,
                        &get_block_shard_uid(&CryptoHash::default(), shard_uid),
                        &chunk_extra,
                    )
                    .unwrap();
            }
            update_for_chunk_extra.commit().unwrap();

            tries.load_mem_tries_for_enabled_shards(&shard_uids, false).unwrap();
        }
        tries
    }
}

pub fn test_populate_trie(
    tries: &ShardTries,
    root: &CryptoHash,
    shard_uid: ShardUId,
    changes: Vec<(Vec<u8>, Option<Vec<u8>>)>,
) -> CryptoHash {
    let trie = tries.get_trie_for_shard(shard_uid, *root);
    let trie_changes = trie.update(changes.iter().cloned()).unwrap();
    let mut store_update = tries.store_update();
    tries.apply_memtrie_changes(&trie_changes, shard_uid, 1); // TODO: don't hardcode block height
    let root = tries.apply_all(&trie_changes, shard_uid, &mut store_update);
    store_update.commit().unwrap();
    let deduped = simplify_changes(&changes);
    let trie = tries.get_trie_for_shard(shard_uid, root);
    for (key, value) in deduped {
        assert_eq!(trie.get(&key), Ok(value));
    }
    root
}

pub fn test_populate_flat_storage(
    tries: &ShardTries,
    shard_uid: ShardUId,
    block_hash: &CryptoHash,
    prev_block_hash: &CryptoHash,
    changes: &Vec<(Vec<u8>, Option<Vec<u8>>)>,
) {
    let mut store_update = tries.get_store().flat_store().store_update();
    store_update.set_flat_storage_status(
        shard_uid,
        crate::flat::FlatStorageStatus::Ready(FlatStorageReadyStatus {
            flat_head: BlockInfo { hash: *block_hash, prev_hash: *prev_block_hash, height: 1 },
        }),
    );
    for (key, value) in changes {
        store_update.set(
            shard_uid,
            key.clone(),
            value.as_ref().map(|value| FlatStateValue::on_disk(value)),
        );
    }
    store_update.commit().unwrap();
}

/// Insert values to non-reference-counted columns in the store.
pub fn test_populate_store(store: &Store, data: impl Iterator<Item = (DBCol, Vec<u8>, Vec<u8>)>) {
    let mut update = store.store_update();
    for (column, key, value) in data {
        update.insert(column, key, value);
    }
    update.commit().expect("db commit failed");
}

/// Insert values to reference-counted columns in the store.
pub fn test_populate_store_rc(store: &Store, data: &[(DBCol, Vec<u8>, Vec<u8>)]) {
    let mut update = store.store_update();
    for (column, key, value) in data {
        update.increment_refcount(*column, key, value);
    }
    update.commit().expect("db commit failed");
}

fn gen_alphabet() -> Vec<u8> {
    let alphabet = 'a'..='z';
    alphabet.map(|c| c as u8).collect_vec()
}

fn gen_accounts_from_alphabet(
    rng: &mut impl Rng,
    min_size: usize,
    max_size: usize,
    alphabet: &[u8],
) -> Vec<AccountId> {
    let size = rng.gen_range(min_size..=max_size);
    std::iter::repeat_with(|| gen_account_from_alphabet(rng, alphabet)).take(size).collect()
}

pub fn gen_account_from_alphabet(rng: &mut impl Rng, alphabet: &[u8]) -> AccountId {
    let str_length = rng.gen_range(4..8);
    let s: Vec<u8> = (0..str_length).map(|_| *alphabet.choose(rng).unwrap()).collect();
    from_utf8(&s).unwrap().parse().unwrap()
}

pub fn gen_account(rng: &mut impl Rng) -> AccountId {
    let alphabet = gen_alphabet();
    gen_account_from_alphabet(rng, &alphabet)
}

pub fn gen_unique_accounts(rng: &mut impl Rng, min_size: usize, max_size: usize) -> Vec<AccountId> {
    let alphabet = gen_alphabet();
    let mut accounts = gen_accounts_from_alphabet(rng, min_size, max_size, &alphabet);
    accounts.sort();
    accounts.dedup();
    accounts.shuffle(rng);
    accounts
}

// returns one account for each shard
pub fn gen_shard_accounts() -> Vec<AccountId> {
    vec!["aaa", "aurora", "aurora-0", "kkuuue2akv_1630967379.near", "tge-lockup.sweat"]
        .into_iter()
        .map(AccountId::from_str)
        .map(Result::unwrap)
        .collect()
}

pub fn gen_receipts(rng: &mut impl Rng, max_size: usize) -> Vec<Receipt> {
    let alphabet = gen_alphabet();
    let accounts = gen_accounts_from_alphabet(rng, 1, max_size, &alphabet);
    accounts
        .iter()
        .map(|account_id| {
            Receipt::V1(ReceiptV1 {
                predecessor_id: account_id.clone(),
                receiver_id: account_id.clone(),
                receipt_id: CryptoHash::default(),
                receipt: ReceiptEnum::Data(DataReceipt {
                    data_id: CryptoHash::default(),
                    data: None,
                }),
                priority: 0,
            })
        })
        .collect()
}

pub fn gen_timeouts(rng: &mut impl Rng, max_size: usize) -> Vec<PromiseYieldTimeout> {
    let alphabet = gen_alphabet();
    let accounts = gen_accounts_from_alphabet(rng, 1, max_size, &alphabet);
    accounts
        .iter()
        .map(|account_id| PromiseYieldTimeout {
            account_id: account_id.clone(),
            data_id: CryptoHash::default(),
            expires_at: 0,
        })
        .collect()
}

/// Generates up to max_size random sequence of changes: both insertion and deletions.
/// Deletions are represented as (key, None).
/// Keys are randomly constructed from alphabet, and they have max_length size.
fn gen_changes_helper(
    rng: &mut impl Rng,
    alphabet: &[u8],
    max_size: usize,
    max_length: u64,
) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut state: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut result = Vec::new();
    let delete_probability = rng.gen_range(0.1..0.5);
    let size = rng.gen_range(0..max_size) + 1;
    for _ in 0..size {
        let key_length = rng.gen_range(1..max_length);
        let key: Vec<u8> = (0..key_length).map(|_| *alphabet.choose(rng).unwrap()).collect();

        let delete = rng.gen_range(0.0..1.0) < delete_probability;
        if delete {
            let mut keys: Vec<_> = state.keys().cloned().collect();
            keys.push(key);
            let key = keys.choose(rng).unwrap().clone();
            state.remove(&key);
            result.push((key.clone(), None));
        } else {
            let value_length = rng.gen_range(1..max_length);
            let value: Vec<u8> =
                (0..value_length).map(|_| *alphabet.choose(rng).unwrap()).collect();
            result.push((key.clone(), Some(value.clone())));
            state.insert(key, value);
        }
    }
    result
}

pub fn gen_changes(rng: &mut impl Rng, max_size: usize) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let alphabet = gen_alphabet();
    let max_length = rng.gen_range(2..8);
    gen_changes_helper(rng, &alphabet, max_size, max_length)
}

pub fn gen_larger_changes(rng: &mut impl Rng, max_size: usize) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let alphabet = gen_alphabet();
    let max_length = rng.gen_range(10..20);
    gen_changes_helper(rng, &alphabet, max_size, max_length)
}

pub fn simplify_changes(changes: &[(Vec<u8>, Option<Vec<u8>>)]) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut state: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for (key, value) in changes.iter() {
        if let Some(value) = value {
            state.insert(key.clone(), value.clone());
        } else {
            state.remove(key);
        }
    }
    let mut result: Vec<_> = state.into_iter().map(|(k, v)| (k, Some(v))).collect();
    result.sort();
    result
}

pub fn get_all_delayed_receipts(
    tries: &ShardTries,
    shard_uid: &ShardUId,
    state_root: &StateRoot,
) -> Vec<Receipt> {
    let state_update = &tries.new_trie_update(*shard_uid, *state_root);
    let mut delayed_receipt_indices = get_delayed_receipt_indices(state_update).unwrap();

    let mut receipts = vec![];
    while delayed_receipt_indices.first_index < delayed_receipt_indices.next_available_index {
        let key = TrieKey::DelayedReceipt { index: delayed_receipt_indices.first_index };
        let receipt = get(state_update, &key).unwrap().unwrap();
        delayed_receipt_indices.first_index += 1;
        receipts.push(receipt);
    }
    receipts
}

pub fn get_all_promise_yield_timeouts(
    tries: &ShardTries,
    shard_uid: &ShardUId,
    state_root: &StateRoot,
) -> Vec<PromiseYieldTimeout> {
    let state_update = &tries.new_trie_update(*shard_uid, *state_root);
    let mut promise_yield_indices = get_promise_yield_indices(state_update).unwrap();

    let mut timeouts = vec![];
    while promise_yield_indices.first_index < promise_yield_indices.next_available_index {
        let key = TrieKey::PromiseYieldTimeout { index: promise_yield_indices.first_index };
        let timeout = get(state_update, &key).unwrap().unwrap();
        promise_yield_indices.first_index += 1;
        timeouts.push(timeout);
    }
    timeouts
}
