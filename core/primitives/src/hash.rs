extern crate serde;
extern crate bincode;
extern crate exonum_sodiumoxide as sodiumoxide;

use self::serde::Serialize;
use self::bincode::serialize;

pub type CryptoHash = sodiumoxide::crypto::hash::sha256::Digest;

/// Calculates a hash of a bytes slice.
///
/// # Examples
///
/// The example below calculates the hash of the indicated data.
///
/// ```
/// # extern crate primitives;
///
/// let data = [1, 2, 3];
/// let hash = primitives::hash::hash(&data);
/// ```
pub fn hash(data: &[u8]) -> CryptoHash {
    sodiumoxide::crypto::hash::sha256::hash(data)
}

pub fn hash_struct<T: Serialize>(obj: &T) -> CryptoHash {
    hash(&serialize(&obj).expect("Serialization failed"))
}