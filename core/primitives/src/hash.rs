extern crate exonum_sodiumoxide as sodiumoxide;

pub use hash::sodiumoxide::crypto::hash::sha256::Digest as HashValue;

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
pub fn hash(data: &[u8]) -> HashValue {
    sodiumoxide::crypto::hash::sha256::hash(data)
}
