//! A merkle tree, mirroring `block_merkle_tree`, over the reconstructed lite views
//! of certified spice blocks. Its root is the header's `certified_block_merkle_root`.

use crate::lightclient::reconstruct_certified_lite_view;
use crate::spice::core::{SpiceCoreReader, find_newly_certified_block_hashes};
use crate::{ChainStoreAccess, ChainStoreUpdate};
use near_chain_primitives::Error;
use near_primitives::block::Block;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::types::{CertifiedBlockAccumulatorState, CertifiedBlockLeaf};
use near_store::adapter::chain_store::ChainStoreAdapter;

/// Builds and saves the fork-local certified accumulator state for `block` (prev's tree
/// plus `block`'s newly-certified leaves), keyed by `block`'s hash.
pub fn save_certified_block_merkle_tree_for_block(
    chain_store_update: &mut ChainStoreUpdate,
    reader: &SpiceCoreReader,
    block: &Block,
) -> Result<(), Error> {
    let (tree, newly_certified_leaves) =
        build_certified_block_merkle_tree(chain_store_update.chain_store(), reader, block)?;
    chain_store_update.save_certified_block_merkle_tree(
        *block.header().hash(),
        CertifiedBlockAccumulatorState { tree, newly_certified_leaves },
    );
    Ok(())
}

/// Returns the certified accumulator tree as of `block`, plus one entry per
/// newly-certified leaf.
fn build_certified_block_merkle_tree(
    chain_store: &ChainStoreAdapter,
    reader: &SpiceCoreReader,
    block: &Block,
) -> Result<(PartialMerkleTree, Vec<CertifiedBlockLeaf>), Error> {
    if block.header().is_genesis() {
        return Ok((PartialMerkleTree::default(), vec![]));
    }
    let prev_hash = block.header().prev_hash();
    let prev_header = chain_store.get_block_header(prev_hash)?;
    // Genesis and the pre-spice block at the activation boundary anchor an empty accumulator.
    let prev_tree = if prev_header.is_genesis() || !prev_header.is_spice() {
        PartialMerkleTree::default()
    } else {
        chain_store.get_certified_block_merkle_tree(prev_hash)?
    };
    let prev_uncertified = reader.get_uncertified_chunks(prev_hash)?;
    let newly_certified =
        find_newly_certified_block_hashes(&prev_uncertified, block.spice_core_statements());

    let mut headers = Vec::with_capacity(newly_certified.len());
    for block_hash in &newly_certified {
        headers.push(chain_store.get_block_header(block_hash)?);
    }
    headers.sort_by_key(|header| header.height());

    let mut tree = prev_tree;
    let mut leaves = Vec::with_capacity(headers.len());
    for header in &headers {
        // The chunks this block certifies are still in its uncommitted statements,
        // so the certifying block must overlay them onto the committed results.
        let (state_root, outcome_root) =
            reader.certified_block_roots_for_certifying_block(block, header)?.ok_or_else(|| {
                Error::Other(format!("certified block {} missing execution results", header.hash()))
            })?;
        let leaf_hash = reconstruct_certified_lite_view(header, state_root, outcome_root).hash();
        leaves.push(CertifiedBlockLeaf { certified_block_hash: *header.hash(), leaf_hash });
        tree.insert(leaf_hash);
    }
    Ok((tree, leaves))
}
