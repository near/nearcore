use std::sync::Arc;

use near_chain_primitives::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{
    Direction, MerklePath, MerklePathItem, PartialMerkleTree, combine_hash,
};
use near_primitives::types::NumBlocks;
use near_store::Store;
use near_store::adapter::StoreAdapter;

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

        let mut path = vec![];
        let mut level: u64 = 0;
        let mut index = leaf_index;
        let mut remaining_size = tree_size;

        while remaining_size > 1 {
            // Walk left.
            {
                let cur_index = index;
                let cur_level = level;
                // Go up as long as we're the right child. This finds us a largest subtree for
                // which our current node is the rightmost descendant of at the current level.
                while remaining_size > 1 && index % 2 == 1 {
                    index /= 2;
                    remaining_size = (remaining_size + 1) / 2;
                    level += 1;
                }
                if level > cur_level {
                    // To prove this subtree, get the partial tree for the rightmost leaf of the
                    // subtree. It's OK if we can only go as far as the tree size; we'll aggregate
                    // whatever we can. Once we have the partial tree, we push in whatever is in
                    // between the levels we just traversed.
                    let ordinal = ((cur_index + 1) * (1 << cur_level) - 1).min(tree_size - 1);
                    let partial_tree_for_node = get_block_merkle_tree_from_ordinal(self, ordinal)?;
                    partial_tree_for_node.iter_path_from_bottom(|hash, l| {
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
                // It's possible the right sibling is actually zero, in which case we don't push
                // anything to the path.
                if ordinal >= right_sibling_index * (1 << level) {
                    // To prove a right sibling, get the partial tree for the rightmost leaf of the
                    // subtree, and also get the block hash of the rightmost leaf; combining these
                    // two will give us the root of the subtree, i.e. the right sibling.
                    let leaf_hash = self.get_block_hash_from_ordinal(ordinal)?;
                    let mut subtree_root_hash = leaf_hash;
                    if level > 0 {
                        let partial_tree_for_sibling =
                            get_block_merkle_tree_from_ordinal(self, ordinal)?;
                        partial_tree_for_sibling.iter_path_from_bottom(|hash, l| {
                            if l < level {
                                subtree_root_hash = combine_hash(&hash, &subtree_root_hash);
                            }
                        });
                    }
                    path.push(MerklePathItem {
                        hash: subtree_root_hash,
                        direction: Direction::Right,
                    });
                }

                index = (index + 1) / 2;
                remaining_size = (remaining_size + 1) / 2;
                level += 1;
            }
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

impl MerkleProofAccess for Store {
    fn get_block_merkle_tree(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Arc<PartialMerkleTree>, Error> {
        self.chain_store().get_block_merkle_tree(block_hash).map(Arc::new)
    }

    fn get_block_hash_from_ordinal(&self, block_ordinal: NumBlocks) -> Result<CryptoHash, Error> {
        self.chain_store().get_block_hash_from_ordinal(block_ordinal)
    }
}

#[cfg(test)]
mod tests {
    use super::MerkleProofAccess;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::hash::{CryptoHash, hash};
    use near_primitives::merkle::{MerklePath, PartialMerkleTree, verify_hash};
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
            // cspell:words provee
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
