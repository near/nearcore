use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use crate::hash::hash;
use crate::types::MerkleHash;

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize, Deserialize)]
pub struct MerklePathItem {
    pub hash: MerkleHash,
    pub direction: Direction,
}

pub type MerklePath = Vec<MerklePathItem>;

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, Serialize, Deserialize)]
pub enum Direction {
    Left,
    Right,
}

pub fn combine_hash(hash1: MerkleHash, hash2: MerkleHash) -> MerkleHash {
    let mut combined: Vec<u8> = hash1.into();
    combined.append(&mut hash2.into());
    hash(&combined)
}

/// Merklize an array of items. If the array is empty, returns hash of 0
pub fn merklize<T: BorshSerialize>(arr: &[T]) -> (MerkleHash, Vec<MerklePath>) {
    if arr.is_empty() {
        return (MerkleHash::default(), vec![]);
    }
    let mut len = (arr.len() as u32).next_power_of_two();
    let mut hashes: Vec<_> = (0..len)
        .map(|i| {
            if i < arr.len() as u32 {
                hash(&arr[i as usize].try_to_vec().expect("Failed to serialize"))
            } else {
                hash(&[0])
            }
        })
        .collect();
    // degenerate case
    if len == 1 {
        return (hashes[0], vec![vec![]]);
    }
    let mut paths: Vec<MerklePath> = (0..arr.len())
        .map(|i| {
            if i % 2 == 0 {
                vec![MerklePathItem { hash: hashes[(i + 1) as usize], direction: Direction::Right }]
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
            let hash = combine_hash(hashes[2 * i as usize], hashes[(2 * i + 1) as usize]);
            hashes[i as usize] = hash;
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
    }
    (hashes[0], paths)
}

/// Verify merkle path for given item and corresponding path.
pub fn verify_path<T: BorshSerialize>(root: MerkleHash, path: &MerklePath, item: &T) -> bool {
    let mut hash = hash(&item.try_to_vec().expect("Failed to serialize"));
    for item in path {
        match item.direction {
            Direction::Left => {
                hash = combine_hash(item.hash, hash);
            }
            Direction::Right => {
                hash = combine_hash(hash, item.hash);
            }
        }
    }
    hash == root
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    fn test_with_len(n: u32, rng: &mut StdRng) {
        let mut arr: Vec<u32> = vec![];
        for _ in 0..n {
            arr.push(rng.gen_range(0, 1000));
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
            let len: u32 = rng.gen_range(1, 50);
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
}
