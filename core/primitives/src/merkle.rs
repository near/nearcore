use crate::hash::CryptoHash;
use crate::types::MerkleHash;
use borsh::{BorshDeserialize, BorshSerialize};

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct MerklePathItem {
    pub hash: MerkleHash,
    pub direction: Direction,
}

pub type MerklePath = Vec<MerklePathItem>;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum Direction {
    Left,
    Right,
}

pub fn combine_hash(hash1: &MerkleHash, hash2: &MerkleHash) -> MerkleHash {
    CryptoHash::hash_borsh((hash1, hash2))
}

/// Merklize an array of items. If the array is empty, returns hash of 0
pub fn merklize<T: BorshSerialize>(arr: &[T]) -> (MerkleHash, Vec<MerklePath>) {
    if arr.is_empty() {
        return (MerkleHash::default(), vec![]);
    }
    let mut len = arr.len().next_power_of_two();
    let mut hashes = arr.iter().map(CryptoHash::hash_borsh).collect::<Vec<_>>();

    // degenerate case
    if len == 1 {
        return (hashes[0], vec![vec![]]);
    }
    let mut arr_len = arr.len();
    let mut paths: Vec<MerklePath> = (0..arr_len)
        .map(|i| {
            if i % 2 == 0 {
                if i + 1 < arr_len {
                    vec![MerklePathItem {
                        hash: hashes[(i + 1) as usize],
                        direction: Direction::Right,
                    }]
                } else {
                    vec![]
                }
            } else {
                vec![MerklePathItem { hash: hashes[(i - 1) as usize], direction: Direction::Left }]
            }
        })
        .collect();

    let mut counter = 1;
    while len > 1 {
        len /= 2;
        counter *= 2;
        for i in 0..len {
            let hash = if 2 * i >= arr_len {
                continue;
            } else if 2 * i + 1 >= arr_len {
                hashes[2 * i]
            } else {
                combine_hash(&hashes[2 * i], &hashes[2 * i + 1])
            };
            hashes[i] = hash;
            if len > 1 {
                if i % 2 == 0 {
                    for j in 0..counter {
                        let index = ((i + 1) * counter + j) as usize;
                        if index < arr.len() {
                            paths[index].push(MerklePathItem { hash, direction: Direction::Left });
                        }
                    }
                } else {
                    for j in 0..counter {
                        let index = ((i - 1) * counter + j) as usize;
                        if index < arr.len() {
                            paths[index].push(MerklePathItem { hash, direction: Direction::Right });
                        }
                    }
                }
            }
        }
        arr_len = (arr_len + 1) / 2;
    }
    (hashes[0], paths)
}

/// Verify merkle path for given item and corresponding path.
pub fn verify_path<T: BorshSerialize>(root: MerkleHash, path: &MerklePath, item: T) -> bool {
    verify_hash(root, path, CryptoHash::hash_borsh(item))
}

pub fn verify_hash(root: MerkleHash, path: &MerklePath, item_hash: MerkleHash) -> bool {
    compute_root_from_path(path, item_hash) == root
}

pub fn compute_root_from_path(path: &MerklePath, item_hash: MerkleHash) -> MerkleHash {
    let mut res = item_hash;
    for item in path {
        match item.direction {
            Direction::Left => {
                res = combine_hash(&item.hash, &res);
            }
            Direction::Right => {
                res = combine_hash(&res, &item.hash);
            }
        }
    }
    res
}

pub fn compute_root_from_path_and_item<T: BorshSerialize>(
    path: &MerklePath,
    item: T,
) -> MerkleHash {
    compute_root_from_path(path, CryptoHash::hash_borsh(item))
}

/// Merkle tree that only maintains the path for the next leaf, i.e,
/// when a new leaf is inserted, the existing `path` is its proof.
/// The root can be computed by folding `path` from right but is not explicitly
/// maintained to save space.
/// The size of the object is O(log(n)) where n is the number of leaves in the tree, i.e, `size`.
#[derive(
    Default, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq, Debug, serde::Serialize,
)]
pub struct PartialMerkleTree {
    /// Path for the next leaf.
    path: Vec<MerkleHash>,
    /// Number of leaves in the tree.
    size: u64,
}

impl PartialMerkleTree {
    pub fn root(&self) -> MerkleHash {
        if self.path.is_empty() {
            CryptoHash::default()
        } else {
            let mut res = *self.path.last().unwrap();
            let len = self.path.len();
            for i in (0..len - 1).rev() {
                res = combine_hash(&self.path[i], &res);
            }
            res
        }
    }

    pub fn insert(&mut self, elem: MerkleHash) {
        let mut s = self.size;
        let mut node = elem;
        while s % 2 == 1 {
            let last_path_elem = self.path.pop().unwrap();
            node = combine_hash(&last_path_elem, &node);
            s /= 2;
        }
        self.path.push(node);
        self.size += 1;
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn get_path(&self) -> &[MerkleHash] {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    fn test_with_len(n: u32, rng: &mut StdRng) {
        let mut arr: Vec<u32> = vec![];
        for _ in 0..n {
            arr.push(rng.gen_range(0..1000));
        }
        let (root, paths) = merklize(&arr);
        assert_eq!(paths.len() as u32, n);
        for (i, item) in arr.iter().enumerate() {
            assert!(verify_path(root, &paths[i], item));
        }
    }

    #[test]
    fn test_merkle_path() {
        let mut rng: StdRng = SeedableRng::seed_from_u64(1);
        for _ in 0..10 {
            let len: u32 = rng.gen_range(1..100);
            test_with_len(len, &mut rng);
        }
    }

    #[test]
    fn test_incorrect_path() {
        let items = vec![111, 222, 333];
        let (root, paths) = merklize(&items);
        for i in 0..items.len() {
            assert!(!verify_path(root, &paths[(i + 1) % 3], &items[i]))
        }
    }

    #[test]
    fn test_elements_order() {
        let items = vec![1, 2];
        let (root, _) = merklize(&items);
        let items2 = vec![2, 1];
        let (root2, _) = merklize(&items2);
        assert_ne!(root, root2);
    }

    /// Compute the merkle root of a given array.
    fn compute_root(hashes: &[CryptoHash]) -> CryptoHash {
        if hashes.is_empty() {
            CryptoHash::default()
        } else if hashes.len() == 1 {
            hashes[0]
        } else {
            let len = hashes.len();
            let subtree_len = len.next_power_of_two() / 2;
            let left_root = compute_root(&hashes[0..subtree_len]);
            let right_root = compute_root(&hashes[subtree_len..len]);
            combine_hash(&left_root, &right_root)
        }
    }

    #[test]
    fn test_merkle_tree() {
        let mut tree = PartialMerkleTree::default();
        let mut hashes = vec![];
        for i in 0..50 {
            assert_eq!(compute_root(&hashes), tree.root());
            let cur_hash = CryptoHash::hash_bytes(&[i]);
            hashes.push(cur_hash);
            tree.insert(cur_hash);
        }
    }

    #[test]
    fn test_combine_hash_stability() {
        let a = MerkleHash::default();
        let b = MerkleHash::default();
        let cc = combine_hash(&a, &b);
        assert_eq!(
            cc.0,
            [
                245, 165, 253, 66, 209, 106, 32, 48, 39, 152, 239, 110, 211, 9, 151, 155, 67, 0,
                61, 35, 32, 217, 240, 232, 234, 152, 49, 169, 39, 89, 251, 75
            ]
        );
    }
}
