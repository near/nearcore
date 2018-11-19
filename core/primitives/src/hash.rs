use exonum_sodiumoxide::{self as sodiumoxide, crypto::hash::sha256::Digest};
use serde::Serialize;
use traits::Encode;

pub type CryptoHash = Digest;

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
    hash(&obj.encode().expect("Serialization failed"))
}
