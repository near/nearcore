use super::sharding::{next_block_has_new_shard_layout, this_block_has_new_shard_layout};
use super::LoopActionFn;
use crate::test_loop::env::TestData;
use crate::test_loop::utils::sharding::get_memtrie_for_shard;
use near_async::test_loop::data::{TestLoopData, TestLoopDataHandle};
use near_chain::types::Tip;
use near_chain::ChainStoreAccess;
use near_client::client_actor::ClientActorInner;
use near_client::Client;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    BufferedReceiptIndices, DelayedReceiptIndices, PromiseYieldIndices,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::AccountId;
use near_store::{get, ShardUId};

pub enum ReceiptKind {
    Delayed,
    Buffered,
    PromiseYield,
}

/// Checks that the shards containing `accounts` have a non empty set of receipts
/// of type `kind` at the resharding block.
pub fn check_receipts_presence_at_resharding_block(
    accounts: Vec<AccountId>,
    kind: ReceiptKind,
) -> LoopActionFn {
    Box::new(
        move |_: &[TestData],
              test_loop_data: &mut TestLoopData,
              client_handle: TestLoopDataHandle<ClientActorInner>| {
            let client_actor = test_loop_data.get_mut(&client_handle);
            let tip = client_actor.client.chain.head().unwrap();

            if !next_block_has_new_shard_layout(client_actor.client.epoch_manager.as_ref(), &tip) {
                return;
            }

            accounts.iter().for_each(|account| {
                check_receipts_at_block(client_actor, &account, &kind, tip.clone())
            });
        },
    )
}

/// Checks that the shards containing `accounts` have a non empty set of receipts
/// of type `kind` at the block after the resharding block.
pub fn check_receipts_presence_after_resharding_block(
    accounts: Vec<AccountId>,
    kind: ReceiptKind,
) -> LoopActionFn {
    Box::new(
        move |_: &[TestData],
              test_loop_data: &mut TestLoopData,
              client_handle: TestLoopDataHandle<ClientActorInner>| {
            let client_actor = test_loop_data.get_mut(&client_handle);
            let tip = client_actor.client.chain.head().unwrap();

            if !this_block_has_new_shard_layout(client_actor.client.epoch_manager.as_ref(), &tip) {
                return;
            }

            accounts.iter().for_each(|account| {
                check_receipts_at_block(client_actor, &account, &kind, tip.clone())
            });
        },
    )
}

/// Asserts the presence of any receipt of type `kind` at the provided chain `tip`.
pub fn check_receipts_at_block(
    client_actor: &mut ClientActorInner,
    account: &AccountId,
    kind: &ReceiptKind,
    tip: Tip,
) {
    let epoch_manager = &client_actor.client.epoch_manager;
    let shard_layout = epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    let shard_id = epoch_manager.account_id_to_shard_id(&account, &tip.epoch_id).unwrap();
    let shard_uid = &ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
    let congestion_info = &client_actor
        .client
        .chain
        .chain_store()
        .get_chunk_extra(&tip.last_block_hash, shard_uid)
        .unwrap()
        .congestion_info()
        .unwrap();

    let num_shards = shard_layout.shard_ids().count();
    let has_delayed = congestion_info.delayed_receipts_gas() != 0;
    let has_buffered = congestion_info.buffered_receipts_gas() != 0;
    tracing::info!(target: "test", height=tip.height, num_shards, ?shard_id, has_delayed, has_buffered, "checking receipts");

    match kind {
        ReceiptKind::Delayed => {
            assert!(has_delayed);
            check_delayed_receipts_exist_in_memtrie(
                &client_actor.client,
                &shard_uid,
                &tip.prev_block_hash,
            );
        }
        ReceiptKind::Buffered => {
            assert!(has_buffered);
            check_buffered_receipts_exist_in_memtrie(
                &client_actor.client,
                &shard_uid,
                &tip.prev_block_hash,
            );
        }
        ReceiptKind::PromiseYield => check_promise_yield_receipts_exist_in_memtrie(
            &client_actor.client,
            &shard_uid,
            &tip.prev_block_hash,
        ),
    }
}

/// Asserts that a non zero amount of delayed receipts exist in MemTrie for the given shard.
fn check_delayed_receipts_exist_in_memtrie(
    client: &Client,
    shard_uid: &ShardUId,
    prev_block_hash: &CryptoHash,
) {
    let memtrie = get_memtrie_for_shard(client, shard_uid, prev_block_hash);
    let indices: DelayedReceiptIndices =
        get(&memtrie, &TrieKey::DelayedReceiptIndices).unwrap().unwrap();
    assert_ne!(indices.len(), 0);
}

/// Asserts that a non zero amount of buffered receipts exist in MemTrie for the given shard.
fn check_buffered_receipts_exist_in_memtrie(
    client: &Client,
    shard_uid: &ShardUId,
    prev_block_hash: &CryptoHash,
) {
    let memtrie = get_memtrie_for_shard(client, shard_uid, prev_block_hash);
    let indices: BufferedReceiptIndices =
        get(&memtrie, &TrieKey::BufferedReceiptIndices).unwrap().unwrap();
    // There should be at least one buffered receipt going to some other shard. It's not very precise but good enough.
    assert_ne!(indices.shard_buffers.values().fold(0, |acc, buffer| acc + buffer.len()), 0);
}

/// Asserts that a non zero amount of promise yield receipts exist in MemTrie for the given shard.
fn check_promise_yield_receipts_exist_in_memtrie(
    client: &Client,
    shard_uid: &ShardUId,
    prev_block_hash: &CryptoHash,
) {
    let memtrie = get_memtrie_for_shard(client, shard_uid, prev_block_hash);
    let indices: PromiseYieldIndices =
        get(&memtrie, &TrieKey::PromiseYieldIndices).unwrap().unwrap();
    assert_ne!(indices.len(), 0);
}
