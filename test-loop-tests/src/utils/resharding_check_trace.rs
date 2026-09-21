//! Records every check and action of the resharding tests under the `resharding_check` tracing
//! target, so a migrated test can be compared with the version before it.
//!
//! This module exists only for that comparison. It is removed once the migration is done.

use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, EpochHeight, NumShards, ShardId};
use near_store::ShardUId;

/// The sample every slowest-node check of a block runs on.
pub fn sample(
    height: BlockHeight,
    hash: &CryptoHash,
    num_shards: NumShards,
    clock_node: &AccountId,
    read_node: &AccountId,
) {
    tracing::info!(target: "resharding_check", check = "sample", height, ?hash, num_shards, %clock_node, %read_node);
}

pub fn initial_shard_accounts(height: BlockHeight, num_shards: NumShards) {
    tracing::info!(target: "resharding_check", check = "initial_shard_accounts", height, num_shards);
}

pub fn all_chunks_included(height: BlockHeight, chunk_mask: &[bool]) {
    tracing::info!(target: "resharding_check", check = "all_chunks_included", height, ?chunk_mask);
}

pub fn trie_sanity(
    node: &AccountId,
    height: BlockHeight,
    final_height: BlockHeight,
    hash: &CryptoHash,
    checked_shards: &[ShardUId],
) {
    tracing::info!(target: "resharding_check", check = "trie_sanity", %node, height, final_height, ?hash, ?checked_shards);
}

pub fn trie_sanity_skipped(node: &AccountId, height: BlockHeight, reason: &str) {
    tracing::info!(target: "resharding_check", check = "trie_sanity", %node, height, skipped = reason);
}

pub fn trie_sanity_shard_skipped(node: &AccountId, shard_uid: ShardUId, reason: &str) {
    tracing::info!(target: "resharding_check", check = "trie_sanity_shard", %node, ?shard_uid, skipped = reason);
}

pub fn split_deadline(height: BlockHeight, epoch_height: EpochHeight, epoch_height_limit: u64) {
    tracing::info!(target: "resharding_check", check = "split_deadline", height, epoch_height, epoch_height_limit);
}

pub fn first_layout_change(height: BlockHeight, epoch_height: EpochHeight) {
    tracing::info!(target: "resharding_check", check = "first_layout_change", height, epoch_height);
}

pub fn final_layout(
    height: BlockHeight,
    epoch_height: EpochHeight,
    resharding_block_hash: &CryptoHash,
    num_shards: NumShards,
) {
    tracing::info!(target: "resharding_check", check = "final_layout", height, epoch_height, ?resharding_block_hash, num_shards);
}

pub fn gc_budget(epoch_height: EpochHeight, gc_num_epochs_to_keep: u64, num_epochs_to_wait: u64) {
    tracing::info!(target: "resharding_check", check = "gc_budget", epoch_height, gc_num_epochs_to_keep, num_epochs_to_wait);
}

pub fn validator_stickiness(height: BlockHeight, shard_id: ShardId, num_kept_validators: usize) {
    tracing::info!(target: "resharding_check", check = "validator_stickiness", height, %shard_id, num_kept_validators);
}

pub fn validator_stickiness_skipped(height: BlockHeight, reason: &str) {
    tracing::info!(target: "resharding_check", check = "validator_stickiness", height, skipped = reason);
}

pub fn two_split_epoch_gap(
    first_split_epoch_height: EpochHeight,
    second_split_epoch_height: EpochHeight,
) {
    tracing::info!(target: "resharding_check", check = "two_split_epoch_gap", first_split_epoch_height, second_split_epoch_height);
}

pub fn parent_mapping(
    node: &AccountId,
    sample_height: BlockHeight,
    node_head_height: BlockHeight,
    node_head_hash: &CryptoHash,
    parent_shard_uid: ShardUId,
    num_mapped_children: usize,
) {
    tracing::info!(target: "resharding_check", check = "parent_mapping", %node, sample_height, node_head_height, ?node_head_hash, ?parent_shard_uid, num_mapped_children);
}

pub fn completion(height: BlockHeight, epoch_height: EpochHeight) {
    tracing::info!(target: "resharding_check", check = "completion", height, epoch_height);
}

pub fn action_status(action_index: usize, status: &str) {
    tracing::info!(target: "resharding_check", check = "action_status", action_index, status);
}

pub fn checked_all_epochs(node: &AccountId, height: BlockHeight) {
    tracing::info!(target: "resharding_check", check = "checked_all_epochs", %node, height);
}

pub fn receipt_presence(
    node: &AccountId,
    height: BlockHeight,
    hash: &CryptoHash,
    account: &AccountId,
    shard_uid: ShardUId,
    kind: &str,
    has_delayed: bool,
    has_buffered: bool,
) {
    tracing::info!(target: "resharding_check", check = "receipt_presence", %node, height, ?hash, %account, ?shard_uid, kind, has_delayed, has_buffered);
}

pub fn deleted_account_step(step: &str, height: BlockHeight, account: &AccountId) {
    tracing::info!(target: "resharding_check", check = "deleted_account", step, height, %account);
}

pub fn indices_node(
    height: BlockHeight,
    trie_key: &str,
    parent: &str,
    left_child: &str,
    right_child: &str,
) {
    tracing::info!(target: "resharding_check", check = "indices_node", height, trie_key, parent, left_child, right_child);
}

pub fn transaction_outcome(height: BlockHeight, tx_hash: &CryptoHash, status: &str) {
    tracing::info!(target: "resharding_check", check = "transaction_outcome", height, ?tx_hash, status);
}

pub fn submitted_tx(
    height: BlockHeight,
    signer: &AccountId,
    receiver: &AccountId,
    tx_hash: &CryptoHash,
) {
    tracing::info!(target: "resharding_check", check = "submitted_tx", height, %signer, %receiver, ?tx_hash);
}
