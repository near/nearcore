//! A merkle tree, mirroring `block_merkle_tree`, over the reconstructed lite views
//! of certified spice blocks. Its root is the header's `certified_block_merkle_root`.

use crate::lightclient::reconstruct_certified_lite_view;
use crate::spice::core::{SpiceCoreReader, find_newly_certified_block_hashes};
use crate::{ChainStoreAccess, ChainStoreUpdate};
use near_chain_primitives::Error;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{MerklePath, PartialMerkleTree};
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::merkle_proof::compute_merkle_path_by_ordinal;
use std::sync::Arc;

/// Builds and saves the certified-block merkle tree as of `block`: its prev's tree
/// extended with a leaf per block `block` newly certifies, keyed by `block`'s hash.
pub fn update_and_save_certified_block_merkle_tree(
    chain_store_update: &mut ChainStoreUpdate,
    reader: &SpiceCoreReader,
    block: &Block,
) -> Result<(), Error> {
    let (tree, leaves) =
        build_certified_block_merkle_tree(chain_store_update.chain_store(), reader, block)?;
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
    reader: &SpiceCoreReader,
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
        let leaf = reconstruct_certified_lite_view(header, state_root, outcome_root).hash();
        let ordinal = tree.size();
        leaves.push((ordinal, tree.clone(), leaf, *header.hash()));
        tree.insert(leaf);
    }
    Ok((tree, leaves))
}

/// Inclusion proof of the certified leaf at `leaf_ordinal` within the certified
/// accumulator of size `tree_size`, reading the per-ordinal frontier+leaf from
/// `CertifiedAccumulatorByOrdinal`.
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
    compute_merkle_path_by_ordinal(
        leaf_ordinal,
        tree_size,
        |ordinal| Ok(Arc::new(chain_store.get_certified_accumulator_by_ordinal(ordinal)?.0)),
        |ordinal| Ok(chain_store.get_certified_accumulator_by_ordinal(ordinal)?.1),
    )
}
