use std::{collections::HashMap, sync::Arc};

use near_chain_primitives::Error;
use near_primitives::{
    hash::CryptoHash,
    merkle::{combine_hash, Direction, MerklePath, MerklePathItem, PartialMerkleTree},
    types::{MerkleHash, NumBlocks},
    utils::index_to_bytes,
};
use near_store::{DBCol, Store};

/// Implement block merkle proof retrieval.
///
/// The logic was originally a part of `Chain` implementation.
/// This trait is introduced because we want to support `Store` when we don't have the `ChainStore`,
/// but we want to support `ChainStore`` if we have it so we can make use of the caches.
pub trait MerkleProofAccess {
    fn get_block_merkle_tree(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Arc<PartialMerkleTree>, Error>;

    fn get_block_hash_from_ordinal(&self, block_ordinal: NumBlocks) -> Result<CryptoHash, Error>;

    /// Get merkle proof for block with hash `block_hash` in the merkle tree of `head_block_hash`.
    fn compute_past_block_proof_in_merkle_tree_of_later_block(
        &self,
        block_hash: &CryptoHash,
        head_block_hash: &CryptoHash,
    ) -> Result<MerklePath, Error> {
        let leaf_index = self.get_block_merkle_tree(block_hash)?.size();
        let tree_size = self.get_block_merkle_tree(head_block_hash)?.size();
        if leaf_index >= tree_size {
            if block_hash == head_block_hash {
                // special case if the block to prove is the same as head
                return Ok(vec![]);
            }
            return Err(Error::Other(format!(
                "block {} is ahead of head block {}",
                block_hash, head_block_hash
            )));
        }
        let mut level = 0;
        let mut counter = 1;
        let mut cur_index = leaf_index;
        let mut path = vec![];
        let mut tree_nodes = HashMap::new();
        let mut iter = tree_size;
        while iter > 1 {
            if cur_index % 2 == 0 {
                cur_index += 1
            } else {
                cur_index -= 1;
            }
            let direction = if cur_index % 2 == 0 { Direction::Left } else { Direction::Right };
            let maybe_hash = if cur_index % 2 == 1 {
                // node not immediately available. Needs to be reconstructed
                reconstruct_merkle_tree_node(
                    self,
                    cur_index,
                    level,
                    counter,
                    tree_size,
                    &mut tree_nodes,
                )?
            } else {
                get_merkle_tree_node(self, cur_index, level, counter, tree_size, &mut tree_nodes)?
            };
            if let Some(hash) = maybe_hash {
                path.push(MerklePathItem { hash, direction });
            }
            cur_index /= 2;
            iter = (iter + 1) / 2;
            level += 1;
            counter *= 2;
        }
        Ok(path)
    }
}

fn get_block_merkle_tree_from_ordinal(
    this: &(impl MerkleProofAccess + ?Sized),
    block_ordinal: NumBlocks,
) -> Result<Arc<PartialMerkleTree>, Error> {
    let block_hash = this.get_block_hash_from_ordinal(block_ordinal)?;
    this.get_block_merkle_tree(&block_hash)
}

/// Get node at given position (index, level). If the node does not exist, return `None`.
fn get_merkle_tree_node(
    this: &(impl MerkleProofAccess + ?Sized),
    index: u64,
    level: u64,
    counter: u64,
    tree_size: u64,
    tree_nodes: &mut HashMap<(u64, u64), Option<MerkleHash>>,
) -> Result<Option<MerkleHash>, Error> {
    if let Some(hash) = tree_nodes.get(&(index, level)) {
        Ok(*hash)
    } else {
        if level == 0 {
            let maybe_hash = if index >= tree_size {
                None
            } else {
                Some(this.get_block_hash_from_ordinal(index)?)
            };
            tree_nodes.insert((index, level), maybe_hash);
            Ok(maybe_hash)
        } else {
            let cur_tree_size = (index + 1) * counter;
            let maybe_hash = if cur_tree_size > tree_size {
                if index * counter <= tree_size {
                    let left_hash = get_merkle_tree_node(
                        this,
                        index * 2,
                        level - 1,
                        counter / 2,
                        tree_size,
                        tree_nodes,
                    )?;
                    let right_hash = reconstruct_merkle_tree_node(
                        this,
                        index * 2 + 1,
                        level - 1,
                        counter / 2,
                        tree_size,
                        tree_nodes,
                    )?;
                    combine_maybe_hashes(left_hash, right_hash)
                } else {
                    None
                }
            } else {
                Some(
                    *get_block_merkle_tree_from_ordinal(this, cur_tree_size)?
                        .get_path()
                        .last()
                        .ok_or_else(|| Error::Other("Merkle tree node missing".to_string()))?,
                )
            };
            tree_nodes.insert((index, level), maybe_hash);
            Ok(maybe_hash)
        }
    }
}

/// Reconstruct node at given position (index, level). If the node does not exist, return `None`.
fn reconstruct_merkle_tree_node(
    this: &(impl MerkleProofAccess + ?Sized),
    index: u64,
    level: u64,
    counter: u64,
    tree_size: u64,
    tree_nodes: &mut HashMap<(u64, u64), Option<MerkleHash>>,
) -> Result<Option<MerkleHash>, Error> {
    if let Some(hash) = tree_nodes.get(&(index, level)) {
        Ok(*hash)
    } else {
        if level == 0 {
            let maybe_hash = if index >= tree_size {
                None
            } else {
                Some(this.get_block_hash_from_ordinal(index)?)
            };
            tree_nodes.insert((index, level), maybe_hash);
            Ok(maybe_hash)
        } else {
            let left_hash = get_merkle_tree_node(
                this,
                index * 2,
                level - 1,
                counter / 2,
                tree_size,
                tree_nodes,
            )?;
            let right_hash = reconstruct_merkle_tree_node(
                this,
                index * 2 + 1,
                level - 1,
                counter / 2,
                tree_size,
                tree_nodes,
            )?;
            let maybe_hash = combine_maybe_hashes(left_hash, right_hash);
            tree_nodes.insert((index, level), maybe_hash);

            Ok(maybe_hash)
        }
    }
}

fn combine_maybe_hashes(
    hash1: Option<MerkleHash>,
    hash2: Option<MerkleHash>,
) -> Option<MerkleHash> {
    match (hash1, hash2) {
        (Some(h1), Some(h2)) => Some(combine_hash(&h1, &h2)),
        (Some(h1), None) => Some(h1),
        (None, Some(_)) => {
            debug_assert!(false, "Inconsistent state in merkle proof computation: left node is None but right node exists");
            None
        }
        _ => None,
    }
}

impl MerkleProofAccess for Store {
    fn get_block_merkle_tree(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Arc<PartialMerkleTree>, Error> {
        match self.get_ser::<PartialMerkleTree>(
            DBCol::BlockMerkleTree,
            &borsh::to_vec(&block_hash).unwrap(),
        )? {
            Some(block_merkle_tree) => Ok(Arc::new(block_merkle_tree)),
            None => {
                Err(Error::Other(format!("Could not find merkle proof for block {}", block_hash)))
            }
        }
    }

    fn get_block_hash_from_ordinal(&self, block_ordinal: NumBlocks) -> Result<CryptoHash, Error> {
        self.get_ser::<CryptoHash>(DBCol::BlockOrdinal, &index_to_bytes(block_ordinal))?.ok_or(
            Error::Other(format!("Could not find block hash from ordinal {}", block_ordinal)),
        )
    }
}
