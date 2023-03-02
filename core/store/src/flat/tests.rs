use crate::flat::delta::FlatStateDelta;
use crate::flat::storage::FlatStorage;
use crate::flat::manager::FlatStorageManager;
use crate::flat::store_helper;
use crate::flat::types::{BlockInfo, ChainAccessForFlatStorage, FlatStorageError};
use crate::test_utils::create_test_store;
use crate::StorageError;
use borsh::BorshSerialize;
use near_primitives::borsh::maybestd::collections::HashSet;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::state::ValueRef;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{
    BlockHeight, RawStateChange, RawStateChangesWithTrieKey, StateChangeCause,
};

use assert_matches::assert_matches;
use std::collections::HashMap;

struct MockChain {
    height_to_hashes: HashMap<BlockHeight, CryptoHash>,
    blocks: HashMap<CryptoHash, BlockInfo>,
    head_height: BlockHeight,
}

impl ChainAccessForFlatStorage for MockChain {
    fn get_block_info(&self, block_hash: &CryptoHash) -> BlockInfo {
        self.blocks.get(block_hash).unwrap().clone()
    }

    fn get_block_hashes_at_height(&self, block_height: BlockHeight) -> HashSet<CryptoHash> {
        self.height_to_hashes.get(&block_height).cloned().iter().cloned().collect()
    }
}

impl MockChain {
    fn block_hash(height: BlockHeight) -> CryptoHash {
        hash(&height.try_to_vec().unwrap())
    }

    /// Build a chain with given set of heights and a function mapping block heights to heights of their parents.
    fn build(
        heights: Vec<BlockHeight>,
        get_parent: fn(BlockHeight) -> Option<BlockHeight>,
    ) -> MockChain {
        let height_to_hashes: HashMap<_, _> =
            heights.iter().cloned().map(|height| (height, MockChain::block_hash(height))).collect();
        let blocks = heights
            .iter()
            .cloned()
            .map(|height| {
                let hash = height_to_hashes.get(&height).unwrap().clone();
                let prev_hash = match get_parent(height) {
                    None => CryptoHash::default(),
                    Some(parent_height) => *height_to_hashes.get(&parent_height).unwrap(),
                };
                (hash, BlockInfo { hash, height, prev_hash })
            })
            .collect();
        MockChain { height_to_hashes, blocks, head_height: heights.last().unwrap().clone() }
    }

    // Create a chain with no forks with length n.
    fn linear_chain(n: usize) -> MockChain {
        Self::build((0..n as BlockHeight).collect(), |i| if i == 0 { None } else { Some(i - 1) })
    }

    // Create a linear chain of length n where blocks with odd numbers are skipped:
    // 0 -> 2 -> 4 -> ...
    fn linear_chain_with_skips(n: usize) -> MockChain {
        Self::build((0..n as BlockHeight).map(|i| i * 2).collect(), |i| {
            if i == 0 {
                None
            } else {
                Some(i - 2)
            }
        })
    }

    // Create a chain with two forks, where blocks 1 and 2 have a parent block 0, and each next block H
    // has a parent block H-2:
    // 0 |-> 1 -> 3 -> 5 -> ...
    //   --> 2 -> 4 -> 6 -> ...
    fn chain_with_two_forks(n: usize) -> MockChain {
        Self::build(
            (0..n as BlockHeight).collect(),
            |i| {
                if i == 0 {
                    None
                } else {
                    Some(i.max(2) - 2)
                }
            },
        )
    }

    fn get_block_hash(&self, height: BlockHeight) -> CryptoHash {
        *self.height_to_hashes.get(&height).unwrap()
    }

    /// create a new block on top the current chain head, return the new block hash
    fn create_block(&mut self) -> CryptoHash {
        let hash = MockChain::block_hash(self.head_height + 1);
        self.height_to_hashes.insert(self.head_height + 1, hash);
        self.blocks.insert(
            hash,
            BlockInfo {
                hash,
                height: self.head_height + 1,
                prev_hash: self.get_block_hash(self.head_height),
            },
        );
        self.head_height += 1;
        hash
    }
}

/// Check correctness of creating `FlatStateDelta` from state changes.
#[test]
fn flat_state_delta_creation() {
    let alice_trie_key = TrieKey::ContractCode { account_id: "alice".parse().unwrap() };
    let bob_trie_key = TrieKey::ContractCode { account_id: "bob".parse().unwrap() };
    let carol_trie_key = TrieKey::ContractCode { account_id: "carol".parse().unwrap() };

    let state_changes = vec![
        RawStateChangesWithTrieKey {
            trie_key: alice_trie_key.clone(),
            changes: vec![
                RawStateChange { cause: StateChangeCause::InitialState, data: Some(vec![1, 2]) },
                RawStateChange {
                    cause: StateChangeCause::ReceiptProcessing { receipt_hash: Default::default() },
                    data: Some(vec![3, 4]),
                },
            ],
        },
        RawStateChangesWithTrieKey {
            trie_key: bob_trie_key.clone(),
            changes: vec![
                RawStateChange { cause: StateChangeCause::InitialState, data: Some(vec![5, 6]) },
                RawStateChange {
                    cause: StateChangeCause::ReceiptProcessing { receipt_hash: Default::default() },
                    data: None,
                },
            ],
        },
    ];

    let flat_state_delta = FlatStateDelta::from_state_changes(&state_changes);
    assert_eq!(flat_state_delta.get(&alice_trie_key.to_vec()), Some(Some(ValueRef::new(&[3, 4]))));
    assert_eq!(flat_state_delta.get(&bob_trie_key.to_vec()), Some(None));
    assert_eq!(flat_state_delta.get(&carol_trie_key.to_vec()), None);
}

/// Check that keys related to delayed receipts are not included to `FlatStateDelta`.
#[test]
fn flat_state_delta_delayed_keys() {
    let delayed_trie_key = TrieKey::DelayedReceiptIndices;
    let delayed_receipt_trie_key = TrieKey::DelayedReceipt { index: 1 };

    let state_changes = vec![
        RawStateChangesWithTrieKey {
            trie_key: delayed_trie_key.clone(),
            changes: vec![RawStateChange {
                cause: StateChangeCause::InitialState,
                data: Some(vec![1]),
            }],
        },
        RawStateChangesWithTrieKey {
            trie_key: delayed_receipt_trie_key.clone(),
            changes: vec![RawStateChange {
                cause: StateChangeCause::InitialState,
                data: Some(vec![2]),
            }],
        },
    ];

    let flat_state_delta = FlatStateDelta::from_state_changes(&state_changes);
    assert!(flat_state_delta.get(&delayed_trie_key.to_vec()).is_none());
    assert!(flat_state_delta.get(&delayed_receipt_trie_key.to_vec()).is_none());
}

/// Check that merge of `FlatStateDelta`s overrides the old changes for the same keys and doesn't conflict with
/// different keys.
#[test]
fn flat_state_delta_merge() {
    let mut delta = FlatStateDelta::from([
        (vec![1], Some(ValueRef::new(&[4]))),
        (vec![2], Some(ValueRef::new(&[5]))),
        (vec![3], None),
        (vec![4], Some(ValueRef::new(&[6]))),
    ]);
    let delta_new = FlatStateDelta::from([
        (vec![2], Some(ValueRef::new(&[7]))),
        (vec![3], Some(ValueRef::new(&[8]))),
        (vec![4], None),
        (vec![5], Some(ValueRef::new(&[9]))),
    ]);
    delta.merge(&delta_new);

    assert_eq!(delta.get(&[1]), Some(Some(ValueRef::new(&[4]))));
    assert_eq!(delta.get(&[2]), Some(Some(ValueRef::new(&[7]))));
    assert_eq!(delta.get(&[3]), Some(Some(ValueRef::new(&[8]))));
    assert_eq!(delta.get(&[4]), Some(None));
    assert_eq!(delta.get(&[5]), Some(Some(ValueRef::new(&[9]))));
}

#[test]
fn block_not_supported_errors() {
    // Create a chain with two forks. Set flat head to be at block 0.
    let chain = MockChain::chain_with_two_forks(5);
    let store = create_test_store();
    let mut store_update = store.store_update();
    store_helper::set_flat_head(&mut store_update, 0, &chain.get_block_hash(0));
    for i in 1..5 {
        store_helper::set_delta(
            &mut store_update,
            0,
            chain.get_block_hash(i),
            &FlatStateDelta::default(),
        )
        .unwrap();
    }
    store_update.commit().unwrap();

    let flat_storage_state = FlatStorage::new(store.clone(), 0, 4, &chain, 0);
    let flat_state_factory = FlatStorageManager::new(store.clone());
    flat_state_factory.add_flat_storage_state_for_shard(0, flat_storage_state);
    let flat_storage_state = flat_state_factory.get_flat_storage_state_for_shard(0).unwrap();

    // Check that flat head can be moved to block 1.
    let flat_head_hash = chain.get_block_hash(1);
    assert_eq!(flat_storage_state.update_flat_head(&flat_head_hash), Ok(()));
    // Check that attempt to move flat head to block 2 results in error because it lays in unreachable fork.
    let fork_block_hash = chain.get_block_hash(2);
    assert_eq!(
        flat_storage_state.update_flat_head(&fork_block_hash),
        Err(FlatStorageError::BlockNotSupported((flat_head_hash, fork_block_hash)))
    );
    // Check that attempt to move flat head to block 0 results in error because it is an unreachable parent.
    let parent_block_hash = chain.get_block_hash(0);
    assert_eq!(
        flat_storage_state.update_flat_head(&parent_block_hash),
        Err(FlatStorageError::BlockNotSupported((flat_head_hash, parent_block_hash)))
    );
    // Check that attempt to move flat head to non-existent block results in the same error.
    let not_existing_hash = hash(&[1, 2, 3]);
    assert_eq!(
        flat_storage_state.update_flat_head(&not_existing_hash),
        Err(FlatStorageError::BlockNotSupported((flat_head_hash, not_existing_hash)))
    );
}

#[test]
fn skipped_heights() {
    // Create a linear chain where some heights are skipped.
    let chain = MockChain::linear_chain_with_skips(5);
    let store = create_test_store();
    let mut store_update = store.store_update();
    store_helper::set_flat_head(&mut store_update, 0, &chain.get_block_hash(0));
    for i in 1..5 {
        store_helper::set_delta(
            &mut store_update,
            0,
            chain.get_block_hash(i * 2),
            &FlatStateDelta::default(),
        )
        .unwrap();
    }
    store_update.commit().unwrap();

    // Check that flat storage state is created correctly for chain which has skipped heights.
    let flat_storage_state = FlatStorage::new(store.clone(), 0, 8, &chain, 0);
    let flat_state_factory = FlatStorageManager::new(store.clone());
    flat_state_factory.add_flat_storage_state_for_shard(0, flat_storage_state);
    let flat_storage_state = flat_state_factory.get_flat_storage_state_for_shard(0).unwrap();

    // Check that flat head can be moved to block 8.
    let flat_head_hash = chain.get_block_hash(8);
    assert_eq!(flat_storage_state.update_flat_head(&flat_head_hash), Ok(()));
}

// This setup tests basic use cases for FlatStorageChunkView and FlatStorage.
// We created a linear chain with no forks, start with flat head at the genesis block, then
// moves the flat head forward, which checking that flat_state.get_ref() still returns the correct
// values and the state is being updated in store.
fn flat_storage_state_sanity(cache_capacity: usize) {
    // 1. Create a chain with 10 blocks with no forks. Set flat head to be at block 0.
    //    Block i sets value for key &[1] to &[i].
    let mut chain = MockChain::linear_chain(10);
    let store = create_test_store();
    let mut store_update = store.store_update();
    store_helper::set_flat_head(&mut store_update, 0, &chain.get_block_hash(0));
    store_helper::set_ref(&mut store_update, vec![1], Some(ValueRef::new(&[0]))).unwrap();
    for i in 1..10 {
        store_helper::set_delta(
            &mut store_update,
            0,
            chain.get_block_hash(i),
            &FlatStateDelta::from([(vec![1], Some(ValueRef::new(&[i as u8])))]),
        )
        .unwrap();
    }
    store_update.commit().unwrap();

    let flat_storage_state = FlatStorage::new(store.clone(), 0, 9, &chain, cache_capacity);
    let flat_state_factory = FlatStorageManager::new(store.clone());
    flat_state_factory.add_flat_storage_state_for_shard(0, flat_storage_state);
    let flat_storage_state = flat_state_factory.get_flat_storage_state_for_shard(0).unwrap();

    // 2. Check that the flat_state at block i reads the value of key &[1] as &[i]
    for i in 0..10 {
        let block_hash = chain.get_block_hash(i);
        let blocks = flat_storage_state.get_blocks_to_head(&block_hash).unwrap();
        assert_eq!(blocks.len(), i as usize);
        let flat_state =
            flat_state_factory.new_flat_state_for_shard(0, Some(block_hash), false).unwrap();
        assert_eq!(flat_state.get_ref(&[1]).unwrap(), Some(ValueRef::new(&[i as u8])));
    }

    // 3. Create a new block that deletes &[1] and add a new value &[2]
    //    Add the block to flat storage.
    let hash = chain.create_block();
    let store_update = flat_storage_state
        .add_block(
            &hash,
            FlatStateDelta::from([(vec![1], None), (vec![2], Some(ValueRef::new(&[1])))]),
            chain.get_block_info(&hash),
        )
        .unwrap();
    store_update.commit().unwrap();

    // 4. Create a flat_state0 at block 10 and flat_state1 at block 4
    //    Verify that they return the correct values
    let blocks = flat_storage_state.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
    assert_eq!(blocks.len(), 10);
    let flat_state0 = flat_state_factory
        .new_flat_state_for_shard(0, Some(chain.get_block_hash(10)), false)
        .unwrap();
    let flat_state1 = flat_state_factory
        .new_flat_state_for_shard(0, Some(chain.get_block_hash(4)), false)
        .unwrap();
    assert_eq!(flat_state0.get_ref(&[1]).unwrap(), None);
    assert_eq!(flat_state0.get_ref(&[2]).unwrap(), Some(ValueRef::new(&[1])));
    assert_eq!(flat_state1.get_ref(&[1]).unwrap(), Some(ValueRef::new(&[4])));
    assert_eq!(flat_state1.get_ref(&[2]).unwrap(), None);
    assert_matches!(store_helper::get_delta(&store, 0, chain.get_block_hash(5)).unwrap(), Some(_));
    assert_matches!(store_helper::get_delta(&store, 0, chain.get_block_hash(10)).unwrap(), Some(_));

    // 5. Move the flat head to block 5, verify that flat_state0 still returns the same values
    // and flat_state1 returns an error. Also check that DBCol::FlatState is updated correctly
    flat_storage_state.update_flat_head(&chain.get_block_hash(5)).unwrap();
    assert_eq!(store_helper::get_ref(&store, &[1]).unwrap(), Some(ValueRef::new(&[5])));
    let blocks = flat_storage_state.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
    assert_eq!(blocks.len(), 5);
    assert_eq!(flat_state0.get_ref(&[1]).unwrap(), None);
    assert_eq!(flat_state0.get_ref(&[2]).unwrap(), Some(ValueRef::new(&[1])));
    assert_matches!(flat_state1.get_ref(&[1]), Err(StorageError::FlatStorageError(_)));
    assert_matches!(store_helper::get_delta(&store, 0, chain.get_block_hash(5)).unwrap(), None);
    assert_matches!(store_helper::get_delta(&store, 0, chain.get_block_hash(10)).unwrap(), Some(_));

    // 6. Move the flat head to block 10, verify that flat_state0 still returns the same values
    //    Also checks that DBCol::FlatState is updated correctly.
    flat_storage_state.update_flat_head(&chain.get_block_hash(10)).unwrap();
    let blocks = flat_storage_state.get_blocks_to_head(&chain.get_block_hash(10)).unwrap();
    assert_eq!(blocks.len(), 0);
    assert_eq!(store_helper::get_ref(&store, &[1]).unwrap(), None);
    assert_eq!(store_helper::get_ref(&store, &[2]).unwrap(), Some(ValueRef::new(&[1])));
    assert_eq!(flat_state0.get_ref(&[1]).unwrap(), None);
    assert_eq!(flat_state0.get_ref(&[2]).unwrap(), Some(ValueRef::new(&[1])));
    assert_matches!(store_helper::get_delta(&store, 0, chain.get_block_hash(10)).unwrap(), None);
}

#[test]
fn flat_storage_state_sanity_cache() {
    flat_storage_state_sanity(100);
}

#[test]
fn flat_storage_state_sanity_no_cache() {
    flat_storage_state_sanity(0);
}

#[test]
fn flat_storage_state_cache_eviction() {
    // 1. Create a simple chain and add single key-value deltas for 3 consecutive blocks.
    let chain = MockChain::linear_chain(4);
    let store = create_test_store();
    let mut store_update = store.store_update();
    store_helper::set_flat_head(&mut store_update, 0, &chain.get_block_hash(0));

    let mut deltas: Vec<(BlockHeight, Vec<u8>, Option<ValueRef>)> = vec![
        (1, vec![1], Some(ValueRef::new(&[1 as u8]))),
        (2, vec![2], None),
        (3, vec![3], Some(ValueRef::new(&[3 as u8]))),
    ];
    for (height, key, value) in deltas.drain(..) {
        store_helper::set_delta(
            &mut store_update,
            0,
            chain.get_block_hash(height),
            &FlatStateDelta::from([(key, value)]),
        )
        .unwrap();
    }
    store_update.commit().unwrap();

    // 2. Create flat storage and apply 3 blocks to it.
    let flat_storage_state = FlatStorage::new(store.clone(), 0, 3, &chain, 2);
    let flat_state_factory = FlatStorageManager::new(store.clone());
    flat_state_factory.add_flat_storage_state_for_shard(0, flat_storage_state);
    let flat_storage_state = flat_state_factory.get_flat_storage_state_for_shard(0).unwrap();
    flat_storage_state.update_flat_head(&chain.get_block_hash(3)).unwrap();

    {
        let mut guard = flat_storage_state.0.write().unwrap();
        // 1st key should be kicked out.
        assert_eq!(guard.get_cached_ref(&[1]), None);
        // For 2nd key, None should be cached.
        assert_eq!(guard.get_cached_ref(&[2]), Some(None));
        // For 3rd key, value should be cached.
        assert_eq!(guard.get_cached_ref(&[3]), Some(Some(ValueRef::new(&[3 as u8]))));
    }

    // Check that value for 1st key is correct, even though it is not in cache.
    assert_eq!(
        flat_storage_state.get_ref(&chain.get_block_hash(3), &[1]),
        Ok(Some(ValueRef::new(&[1 as u8])))
    );

    // After that, 1st key should be added back to LRU cache.
    {
        let mut guard = flat_storage_state.0.write().unwrap();
        assert_eq!(guard.get_cached_ref(&[1]), Some(Some(ValueRef::new(&[1 as u8]))));
    }
}
