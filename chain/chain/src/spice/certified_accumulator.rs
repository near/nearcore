//! The certified-block merkle accumulator: a parallel merkle tree (mirroring
//! `block_merkle_tree`) over the reconstructed light-client lite view of every
//! block whose spice execution results are certified. Its root is committed in
//! the V7 header (`certified_block_merkle_root`) and anchors light-client
//! inclusion proofs. The per-leaf-ordinal index keeps proofs O(log n).

use crate::lightclient::reconstruct_certified_lite_view;
use crate::spice::core::{
    certified_roots_from_results, collect_certified_execution_results_from_ancestry,
    find_newly_certified_block_hashes, get_uncertified_chunks,
};
use crate::{ChainStoreAccess, ChainStoreUpdate};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{
    Direction, MerklePath, MerklePathItem, PartialMerkleTree, combine_hash,
};
use near_primitives::types::{ChunkExecutionResult, ShardId, SpiceChunkId};
use near_primitives::utils::get_execution_results_key;
use near_store::DBCol;
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Builds and saves the certified-block merkle tree as of `block`: the tree as
/// of its prev, extended by the blocks `block` newly certifies (one leaf each,
/// in ascending height order). Keyed by `block`'s hash so the next block's
/// header commits its root. Mirrors `record_uncertified_chunks_for_block`:
/// reads and writes through `chain_store_update`.
pub fn update_and_save_certified_block_merkle_tree(
    chain_store_update: &mut ChainStoreUpdate,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
) -> Result<(), Error> {
    let (tree, leaves) =
        build_certified_block_merkle_tree(chain_store_update.chain_store(), epoch_manager, block)?;
    chain_store_update.save_certified_block_merkle_tree(*block.header().hash(), tree);
    // Per-leaf-ordinal index for light-client inclusion proofs.
    for (ordinal, frontier, leaf, block_hash) in leaves {
        chain_store_update.save_certified_accumulator_entry(ordinal, frontier, leaf);
        chain_store_update.save_certified_block_leaf_ordinal(block_hash, ordinal);
    }
    Ok(())
}

/// Returns the accumulator frontier as of `block`, plus one entry per
/// newly-certified leaf: `(ordinal, frontier-before-leaf, leaf, block_hash)`.
fn build_certified_block_merkle_tree(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
) -> Result<(PartialMerkleTree, Vec<(u64, PartialMerkleTree, CryptoHash, CryptoHash)>), Error> {
    if block.header().is_genesis() {
        return Ok((PartialMerkleTree::default(), vec![]));
    }
    let prev_hash = block.header().prev_hash();
    let prev_tree = if chain_store.get_block_header(prev_hash)?.is_genesis() {
        PartialMerkleTree::default()
    } else {
        chain_store.get_certified_block_merkle_tree(prev_hash)?
    };
    let prev_uncertified = get_uncertified_chunks(chain_store, prev_hash)?;
    let newly_certified =
        find_newly_certified_block_hashes(&prev_uncertified, block.spice_core_statements());

    // Chunks this block itself certifies are only in its (still uncommitted) core
    // statements; overlay them on the committed results so the producer and every
    // validator derive identical leaves.
    let mut overlay: HashMap<SpiceChunkId, &ChunkExecutionResult> = HashMap::new();
    for (chunk_id, result) in block.spice_core_statements().iter_execution_results() {
        overlay.insert(chunk_id.clone(), result);
    }

    let mut headers = Vec::with_capacity(newly_certified.len());
    for block_hash in &newly_certified {
        headers.push(chain_store.get_block_header(block_hash)?);
    }
    headers.sort_by_key(|header| header.height());

    // Gather each newly-certified block's per-shard results from the committed
    // `execution_results` column plus this block's overlay (precedence:
    // overlay > column > ancestry-fallback below).
    let mut results_per_block: Vec<HashMap<ShardId, Arc<ChunkExecutionResult>>> =
        Vec::with_capacity(headers.len());
    let mut needs_ancestry = false;
    for header in &headers {
        let shard_layout = epoch_manager.get_shard_layout(header.epoch_id())?;
        let mut results: HashMap<ShardId, Arc<ChunkExecutionResult>> = HashMap::new();
        for shard_id in shard_layout.shard_ids() {
            let key = get_execution_results_key(header.hash(), shard_id);
            if let Some(result) = chain_store
                .store()
                .caching_get_ser::<ChunkExecutionResult>(DBCol::execution_results(), &key)
            {
                results.insert(shard_id, result);
            }
            let chunk_id = SpiceChunkId { block_hash: *header.hash(), shard_id };
            if let Some(result) = overlay.get(&chunk_id) {
                results.insert(shard_id, Arc::new((*result).clone()));
            }
        }
        needs_ancestry |= shard_layout.shard_ids().any(|shard_id| !results.contains_key(&shard_id));
        results_per_block.push(results);
    }

    // When the async writer is behind, recover the still-missing shards from the
    // ancestry's block bodies. One walk (stopping at the oldest, height-sorted
    // first) covers every newly-certified block at once.
    if needs_ancestry {
        let relevant_blocks: HashSet<CryptoHash> = headers.iter().map(|h| *h.hash()).collect();
        let mut results_by_block = HashMap::new();
        collect_certified_execution_results_from_ancestry(
            chain_store,
            prev_hash,
            &headers[0],
            &relevant_blocks,
            &mut results_by_block,
        )?;
        for (header, results) in headers.iter().zip(results_per_block.iter_mut()) {
            for (shard_id, result) in results_by_block.remove(header.hash()).unwrap_or_default() {
                results.entry(shard_id).or_insert(result);
            }
        }
    }

    let mut tree = prev_tree;
    let mut leaves = Vec::with_capacity(headers.len());
    for (header, results) in headers.iter().zip(results_per_block.iter()) {
        let (state_root, outcome_root) =
            certified_roots_from_results(epoch_manager, header, results)?.ok_or_else(|| {
                Error::Other(format!("certified block {} missing execution results", header.hash()))
            })?;
        let leaf = reconstruct_certified_lite_view(header, state_root, outcome_root).hash();
        let ordinal = tree.size();
        leaves.push((ordinal, tree.clone(), leaf, *header.hash()));
        tree.insert(leaf);
    }
    Ok((tree, leaves))
}

/// Inclusion proof of the certified leaf at `leaf_ordinal` within the certified
/// accumulator of size `tree_size`. Mirrors
/// `MerkleProofAccess::compute_past_block_proof_in_merkle_tree_of_later_block`,
/// reading the per-ordinal frontier+leaf from `CertifiedAccumulatorByOrdinal`.
pub fn compute_certified_block_proof(
    chain_store: &ChainStoreAdapter,
    leaf_ordinal: u64,
    tree_size: u64,
) -> Result<MerklePath, Error> {
    if leaf_ordinal >= tree_size {
        return Err(Error::Other(format!(
            "certified leaf ordinal {leaf_ordinal} is ahead of accumulator size {tree_size}"
        )));
    }

    let mut path = vec![];
    let mut level: u64 = 0;
    let mut index = leaf_ordinal;
    let mut remaining_size = tree_size;

    while remaining_size > 1 {
        // Walk left.
        {
            let cur_index = index;
            let cur_level = level;
            while remaining_size > 1 && index % 2 == 1 {
                index /= 2;
                remaining_size = (remaining_size + 1) / 2;
                level += 1;
            }
            if level > cur_level {
                let ordinal = ((cur_index + 1) * (1 << cur_level) - 1).min(tree_size - 1);
                let (frontier, _) = chain_store.get_certified_accumulator_by_ordinal(ordinal)?;
                frontier.iter_path_from_bottom(|hash, l| {
                    if l >= cur_level && l < level {
                        path.push(MerklePathItem { hash, direction: Direction::Left });
                    }
                });
            }
        }
        // Walk right.
        if remaining_size > 1 {
            let right_sibling_index = index + 1;
            let ordinal = ((right_sibling_index + 1) * (1 << level) - 1).min(tree_size - 1);
            if ordinal >= right_sibling_index * (1 << level) {
                let (frontier, leaf_hash) =
                    chain_store.get_certified_accumulator_by_ordinal(ordinal)?;
                let mut subtree_root_hash = leaf_hash;
                if level > 0 {
                    frontier.iter_path_from_bottom(|hash, l| {
                        if l < level {
                            subtree_root_hash = combine_hash(&hash, &subtree_root_hash);
                        }
                    });
                }
                path.push(MerklePathItem { hash: subtree_root_hash, direction: Direction::Right });
            }

            index = (index + 1) / 2;
            remaining_size = (remaining_size + 1) / 2;
            level += 1;
        }
    }
    Ok(path)
}
