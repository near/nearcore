//! Skipped heights and missing chunks: read as a pattern, made through drop conditions,
//! and attributed to a producer that stopped producing, so a test allows exactly the gaps
//! it caused and still catches any other.

use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use crate::utils::node::TestLoopNode;
use near_chain::{Chain, ChainStoreAccess};
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use near_primitives::version::ProtocolFeature;
use std::collections::{HashMap, HashSet};
use std::ops::Range;

/// The chunk presence of `shard_index` at `heights`: 'x' missing, '.' present.
pub fn observed_chunk_pattern(
    chain: &Chain,
    shard_index: usize,
    heights: Range<BlockHeight>,
) -> String {
    heights
        .map(|height| {
            let block = chain.get_block_by_height(height).unwrap();
            if block.chunks()[shard_index].is_new_chunk(height) { '.' } else { 'x' }
        })
        .collect()
}

/// Drops `shard_index`'s chunk at every height `pattern` marks 'x'. The pattern covers
/// the last `pattern.len()` heights before the spice protocol upgrade, oldest first.
pub fn drop_chunks_marked_missing(
    env: TestLoopEnv,
    shard_index: usize,
    pattern: &str,
) -> TestLoopEnv {
    let first_height_offset = -(pattern.len() as i64);
    pattern.chars().enumerate().filter(|(_, presence)| *presence == 'x').fold(
        env,
        |env, (position, _)| {
            let offset = first_height_offset + position as i64;
            env.drop(DropCondition::ProtocolUpgradeChunkRange(
                ProtocolFeature::Spice.protocol_version(),
                HashMap::from([(shard_index, offset..offset + 1)]),
            ))
        },
    )
}

/// The hash of the highest canonical block of `node` below `height`.
pub fn canonical_hash_below(node: &TestLoopNode, height: BlockHeight) -> CryptoHash {
    let chain = &node.client().chain;
    (1..height)
        .rev()
        .find_map(|below| chain.chain_store.get_block_hash_by_height(below).ok())
        .expect("the genesis block is canonical")
}

/// The block producer of `height` as `node` sees it, whether or not `height` has a block:
/// the epoch of `height` is the one the next block of the canonical block below it is in.
pub fn block_producer_at(node: &TestLoopNode, height: BlockHeight) -> AccountId {
    let epoch_manager = node.client().epoch_manager.as_ref();
    let prev_hash = canonical_hash_below(node, height);
    let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_hash).unwrap();
    epoch_manager.get_block_producer(&epoch_id, height).unwrap()
}

/// What a producer that stopped producing accounts for on the chain, see
/// [`producer_outage`].
#[derive(Debug, Default)]
pub struct ProducerOutage {
    /// The heights of its block slots that have no block.
    pub skipped_heights: HashSet<BlockHeight>,
    /// The chunks it owed that are missing, as `(shard, height of the block lacking it)`.
    pub missing_chunks: HashSet<(ShardId, BlockHeight)>,
}

/// Reads off `node`'s chain what `account_id` accounts for from height `from` on, per thing
/// it produces, once it stopped producing there: its block slots that have no block, until
/// it resumed, and each of its shards' chunks that are missing, until it resumed for that
/// shard. `resumable_after` is the height it may have come back after, a restart or the
/// end of a slowdown; with `None` nothing counts as resuming and the outage runs through
/// the head.
pub fn producer_outage(
    node: &TestLoopNode,
    account_id: &AccountId,
    from: BlockHeight,
    resumable_after: Option<BlockHeight>,
) -> ProducerOutage {
    let chain = &node.client().chain;
    let epoch_manager = node.client().epoch_manager.as_ref();
    let resumable = |height: BlockHeight| resumable_after.is_some_and(|after| height > after);
    let mut outage = ProducerOutage::default();
    let mut block_back: Option<BlockHeight> = None;
    let mut chunks_resumed: HashSet<ShardId> = HashSet::new();
    for height in from..=node.head().height {
        let prev_hash = canonical_hash_below(node, height);
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_hash).unwrap();
        let block = chain.get_block_by_height(height).ok();
        let mut first_block_back = false;
        let own_slot = epoch_manager.get_block_producer(&epoch_id, height).unwrap() == *account_id;
        match block_back {
            Some(back) if height == back + 1 && block.is_none() => {
                outage.skipped_heights.insert(height);
            }
            Some(_) => {}
            None if own_slot => {
                if block.is_none() {
                    outage.skipped_heights.insert(height);
                } else if resumable(height) {
                    block_back = Some(height);
                    first_block_back = true;
                }
            }
            None => {}
        }
        let Some(block) = block else {
            continue;
        };
        let height_created = block.header().prev_height().unwrap_or(0) + 1;
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        for shard_id in shard_layout.shard_ids() {
            let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
            let is_new = block.chunks()[shard_index].is_new_chunk(height);
            if first_block_back && !is_new {
                outage.missing_chunks.insert((shard_id, height));
                continue;
            }
            if chunks_resumed.contains(&shard_id) {
                continue;
            }
            let producer = epoch_manager
                .get_chunk_producer_info_from_prev_block(&prev_hash, shard_id)
                .unwrap();
            if producer.account_id() != account_id {
                continue;
            }
            if !is_new {
                outage.missing_chunks.insert((shard_id, height));
            } else if resumable(height_created) {
                chunks_resumed.insert(shard_id);
            }
        }
    }
    outage
}

/// The `(shard, head height)` pairs at which a transaction for the shard submitted to
/// `node` with that head is forwarded to `account_id` and to no other producer that can
/// still include it soon, so that it is lost when the account cannot take it: a dead
/// node has no pool, and a restarted node still fetching the blocks it missed rejects
/// the transaction as from a fork it does not know. Mirrors the rpc handler's routing.
pub fn forwarding_dead_ends(
    node: &TestLoopNode,
    account_id: &AccountId,
    head_heights: impl IntoIterator<Item = BlockHeight>,
) -> HashSet<(ShardId, BlockHeight)> {
    let epoch_manager = node.client().epoch_manager.as_ref();
    let horizon = node.client().config.tx_routing_height_horizon;
    let mut dead_ends = HashSet::new();
    for head in head_heights {
        let Ok(head_hash) = node.client().chain.chain_store.get_block_hash_by_height(head) else {
            continue;
        };
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&head_hash).unwrap();
        let mut epoch_ids = vec![epoch_id];
        let epoch_start = epoch_manager.get_epoch_start_height(&head_hash).unwrap();
        let epoch_length = epoch_manager.get_epoch_config(&epoch_id).unwrap().epoch_length;
        if head + horizon >= epoch_start + epoch_length {
            epoch_ids.push(epoch_manager.get_next_epoch_id_from_prev_block(&head_hash).unwrap());
        }
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        for shard_id in shard_layout.shard_ids() {
            let near = if horizon >= 3 { 3..=horizon } else { 2..=horizon };
            let targets = near.map(|h| head + h - 1);
            let only_the_account = targets.into_iter().all(|height_created| {
                epoch_ids.iter().all(|epoch_id| {
                    let key = ChunkProductionKey { shard_id, epoch_id: *epoch_id, height_created };
                    epoch_manager
                        .get_chunk_producer_info(&key)
                        .is_ok_and(|producer| producer.account_id() == account_id)
                })
            });
            if only_the_account {
                dead_ends.insert((shard_id, head));
            }
        }
    }
    dead_ends
}
