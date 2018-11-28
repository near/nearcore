use exonum_sodiumoxide::{self as sodiumoxide, crypto::hash::sha256::Digest};
use std::fmt;

use traits::Encode;

#[derive(Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize, Hash)]
#[must_use]
pub struct CryptoHash(pub Digest);

impl CryptoHash {
    pub fn new(data: &[u8]) -> Self {
        let mut d = [0; 32];
        d.copy_from_slice(data);
        CryptoHash { 0: Digest(d) }
    }
}

impl Default for CryptoHash {
    fn default() -> Self {
        CryptoHash(Digest(Default::default()))
    }
}

impl AsRef<[u8]> for CryptoHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsMut<[u8]> for CryptoHash {
    fn as_mut(&mut self) -> &mut [u8] {
        (self.0).0.as_mut()
    }
}

impl fmt::Debug for CryptoHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

impl fmt::Display for CryptoHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

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
    CryptoHash(sodiumoxide::crypto::hash::sha256::hash(data))
}

pub fn hash_struct<T: Encode>(obj: &T) -> CryptoHash {
    hash(&obj.encode().expect("Serialization failed"))
}

impl heapsize::HeapSizeOf for CryptoHash {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}
