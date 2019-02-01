use primitives::hash::{CryptoHash, hash, hash_struct};
use primitives::serialize::Encode;

pub type MerklePath = Vec<(CryptoHash, Direction)>;

#[derive(Debug)]
pub enum Direction {
    Left,
    Right,
}

// TODO: Do this efficiently
fn round_up(x: u32) -> u32 {
    if x == 0 {
        return 0;
    }
    let mut res = 1;
    while res < x {
        res <<= 1;
    }
    return res;
}

fn combine_hash(hash1: CryptoHash, hash2: CryptoHash) -> CryptoHash {
    let mut combined: Vec<u8> = hash1.into();
    combined.append(&mut hash2.into());
    hash(&combined)
}

pub fn merklize<T: Encode>(arr: &[T]) -> (CryptoHash, Vec<MerklePath>) {
    let mut len = round_up(arr.len() as u32);
    let mut hashes: Vec<_> = (0..len).map(|i| {
        if i < arr.len() as u32 {
            hash_struct(&arr[i as usize])
        } else {
            hash_struct(&0)
        }
    }).collect();
    // degenerate case
    if len == 1 {
        return (hashes[0], vec![vec![]]);
    }
    let mut paths: Vec<MerklePath> = (0..arr.len())
        .map(|i| {
            if i % 2 == 0 {
                vec![(hashes[(i+1) as usize], Direction::Right)]
            } else {
                vec![(hashes[(i-1) as usize], Direction::Left)]
            }
        }).collect();
    
    let mut counter = 1;
    while len > 1 {
        len /= 2;
        counter *= 2;
        for i in 0..len {
            let hash = combine_hash(hashes[2*i as usize], hashes[(2*i+1) as usize]);
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

pub fn verify_path<T: Encode>(root: CryptoHash, path: &MerklePath, item: &T) -> bool {
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
    use rand::{random, Rng, StdRng, SeedableRng};

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
        let seed: [u8; 32] = [0; 32];
        let mut rng: StdRng = SeedableRng::from_seed(seed);
        for _ in 0..10 {
            let len: u32 = rng.gen_range(1, 50);
            test_with_len(len, &mut rng);
        }
    }
}