use crate::hash::{hash, hash_struct};
use crate::serialize::Encode;
use crate::types::MerkleHash;

pub type MerklePath = Vec<(MerkleHash, Direction)>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Left,
    Right,
}

fn combine_hash(hash1: MerkleHash, hash2: MerkleHash) -> MerkleHash {
    let mut combined: Vec<u8> = hash1.into();
    combined.append(&mut hash2.into());
    hash(&combined)
}

/// Merklize an array of items. If the array is empty, returns hash of 0
pub fn merklize<T: Encode>(arr: &[T]) -> (MerkleHash, Vec<MerklePath>) {
    if arr.is_empty() {
        return (MerkleHash::default(), vec![]);
    }
    let mut len = (arr.len() as u32).next_power_of_two();
    let mut hashes: Vec<_> = (0..len)
        .map(|i| if i < arr.len() as u32 { hash_struct(&arr[i as usize]) } else { hash_struct(&0) })
        .collect();
    // degenerate case
    if len == 1 {
        return (hashes[0], vec![vec![]]);
    }
    let mut paths: Vec<MerklePath> = (0..arr.len())
        .map(|i| {
            if i % 2 == 0 {
                vec![(hashes[(i + 1) as usize], Direction::Right)]
            } else {
                vec![(hashes[(i - 1) as usize], Direction::Left)]
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
                            paths[index].push((hash, Direction::Left));
                        }
                    }
                } else {
                    for j in 0..counter {
                        let index = ((i - 1) * counter + j) as usize;
                        if index < arr.len() {
                            paths[index].push((hash, Direction::Right));
                        }
                    }
                }
            }
        }
    }
    (hashes[0], paths)
}

/// Verify merkle path for given item and corresponding path.
pub fn verify_path<T: Encode>(root: MerkleHash, path: &MerklePath, item: &T) -> bool {
    let mut hash = hash_struct(item);
    for (h, d) in path {
        match d {
            Direction::Left => {
                hash = combine_hash(*h, hash);
            }
            Direction::Right => {
                hash = combine_hash(hash, *h);
            }
        }
    }
    hash == root
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{SeedableRng, Rng};
    use rand::rngs::StdRng;

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
}
