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
    ///
    /// Guarantees that no block data (PartialMerkleTree or block hash) for any block older than
    /// `block_hash` or newer than `head_block_hash` will be accessed.
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
        let mut level: u64 = 0;
        let mut counter = 1;
        let mut cur_index = leaf_index;
        let mut path = vec![];
        let mut tree_nodes = HashMap::new();
        let mut iter = tree_size;
        // First, keep walking up as long as we're the right child, to get to the root of the
        // largest subtree whose last leaf is the block to prove. We cover this part of the
        // proof using the block's own partial merkle tree, and then proceed to prove this
        // subtree root.
        //
        // Note that this is not strictly necessary for correctness, but it is important in
        // avoiding the need to access any block data older than the block to prove.
        while iter > 1 && cur_index % 2 == 1 {
            cur_index /= 2;
            iter = (iter + 1) / 2;
            level += 1;
            counter *= 2;
        }
        if level > 0 {
            let partial_tree_for_self = self.get_block_merkle_tree(block_hash)?;
            if partial_tree_for_self.get_path().len() < level as usize {
                return Err(Error::Other(format!(
                    "Block {} has {} hashes, expected at least {}",
                    block_hash,
                    partial_tree_for_self.get_path().len(),
                    level
                )));
            }
            for hash in partial_tree_for_self.get_path().iter().rev().take(level as usize) {
                path.push(MerklePathItem { hash: *hash, direction: Direction::Left });
            }
        }

        // Continue to prove the aforementioned subtree root.
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
                // An intermediate node at level L and index I is available in a partial merkle tree
                // stored on disk, if I is even (it is the left child of its parent). Every ordinal
                // between (I+1)*2^L and (I+2)*2^L-1 is able to provide this node (as there is a
                // corresponding 1 at the L-th bit of the binary representation of all these
                // ordinals. To satisfy the requirement that we only access block data between the
                // block to prove and the block to prove against, we need to find an ordinal that is
                // between this range. So we'll arbitrarily opt to use the largest ordinal in this
                // range.
                //
                // Once we've picked the ordinal, we need to locate the node we want in the partial
                // merkle tree for this ordinal. To do that, notice that there is one subtree hash
                // in the partial merkle tree for each 1 in the binary representation of the
                // ordinal. The number of subtrees that are of a lower level than L is the number of
                // 1 bits below the L-th bit of the ordinal. The partial merkle tree is stored in
                // the order from higher level to lower level, so we can find the hash we want by
                // indexing from the end of the partial merkle tree path.
                let last_tree_ordinal_providing_node = cur_tree_size + counter - 1;
                let ordinal_to_provide_node = last_tree_ordinal_providing_node.min(tree_size);
                let merkle_tree =
                    get_block_merkle_tree_from_ordinal(this, ordinal_to_provide_node)?;
                let num_lower_subtree_hashes =
                    (ordinal_to_provide_node - cur_tree_size).count_ones() as usize;
                let hash_index = merkle_tree
                    .get_path()
                    .len()
                    .checked_sub(1 + num_lower_subtree_hashes)
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "Ordinal {} merkle tree has {} hashes, expected at least {}",
                            ordinal_to_provide_node,
                            merkle_tree.get_path().len(),
                            num_lower_subtree_hashes + 1
                        ))
                    })?;
                Some(merkle_tree.get_path()[hash_index])
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

#[cfg(test)]
mod tests {
    use super::MerkleProofAccess;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::hash::{hash, CryptoHash};
    use near_primitives::merkle::{verify_hash, MerklePath, PartialMerkleTree};
    use near_primitives::types::NumBlocks;
    use std::collections::HashMap;
    use std::ops::RangeInclusive;
    use std::sync::Arc;

    struct MerkleProofTestFixture {
        block_merkle_trees: HashMap<CryptoHash, PartialMerkleTree>,
        block_hashes: Vec<CryptoHash>,
        block_merkle_roots: Vec<CryptoHash>,
        last_partial_merkle_tree: PartialMerkleTree,
        tree_size: NumBlocks,
        allowed_access_range: RangeInclusive<NumBlocks>,
    }

    impl MerkleProofAccess for MerkleProofTestFixture {
        fn get_block_merkle_tree(
            &self,
            block_hash: &CryptoHash,
        ) -> Result<Arc<PartialMerkleTree>, near_chain_primitives::Error> {
            let tree = self.block_merkle_trees.get(block_hash).unwrap().clone();
            if !self.allowed_access_range.contains(&tree.size()) {
                panic!("Block partial merkle tree for ordinal {} is not available", tree.size());
            }
            Ok(tree.into())
        }

        fn get_block_hash_from_ordinal(
            &self,
            block_ordinal: NumBlocks,
        ) -> Result<CryptoHash, near_chain_primitives::Error> {
            if !self.allowed_access_range.contains(&block_ordinal) {
                panic!("Block hash for ordinal {} is not available", block_ordinal);
            }
            Ok(self.block_hashes[block_ordinal as usize])
        }
    }

    impl MerkleProofTestFixture {
        fn new() -> Self {
            MerkleProofTestFixture {
                block_merkle_trees: HashMap::new(),
                block_hashes: vec![],
                block_merkle_roots: vec![],
                last_partial_merkle_tree: PartialMerkleTree::default(),
                tree_size: 0,
                allowed_access_range: 0..=NumBlocks::MAX,
            }
        }

        fn append_block(&mut self) {
            let hash = hash(&self.tree_size.to_be_bytes());
            self.block_hashes.push(hash);
            self.block_merkle_roots.push(self.last_partial_merkle_tree.root());
            self.block_merkle_trees.insert(hash, self.last_partial_merkle_tree.clone());
            self.last_partial_merkle_tree.insert(hash);
            self.tree_size += 1;
        }

        fn append_n_blocks(&mut self, n: u64) {
            for _ in 0..n {
                self.append_block();
            }
        }

        fn make_proof(&mut self, index: u64, against: u64) -> MerklePath {
            self.allowed_access_range = index..=against;
            self.compute_past_block_proof_in_merkle_tree_of_later_block(
                &self.block_hashes[index as usize],
                &self.block_hashes[against as usize],
            )
            .unwrap()
        }

        fn verify_proof(&self, index: u64, against: u64, proof: &MerklePath) {
            let provee = self.block_hashes[index as usize];
            let root = self.block_merkle_roots[against as usize];
            assert!(verify_hash(root, proof, provee));
        }
    }

    /// Tests that deriving a merkle proof for block X against merkle root of block Y
    /// requires no PartialMerkleTree or BlockHash data for any block earlier than X
    /// or later than Y.
    ///
    /// This is useful for Epoch Sync where earlier block data are not available.
    #[test]
    fn test_no_dependency_on_blocks_outside_range() {
        init_test_logger();
        let mut f = MerkleProofTestFixture::new();
        const MAX_BLOCKS: u64 = 100;
        f.append_n_blocks(MAX_BLOCKS + 1);
        for i in 0..MAX_BLOCKS {
            for j in i + 1..MAX_BLOCKS {
                println!("Testing proof of block {} against merkle root at {}", i, j);
                let proof = f.make_proof(i, j);
                f.verify_proof(i, j, &proof);
            }
        }
    }
}
