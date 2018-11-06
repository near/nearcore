extern crate exonum_sodiumoxide as sodiumoxide;

// pub use hash::sodiumoxide::crypto::hash::sha256::Digest;

pub struct HashValue(Vec<u8>);

impl HashValue {
    pub fn new(hash_slice: &[u8]) -> Self {
        HashValue{ 0: hash_slice.to_vec() }
    }
}

impl Into<Vec<u8>> for HashValue {
    fn into(self) -> Vec<u8> {
        self.0
    }
}

//impl From<[u8]> for HashValue {
//    fn from(item: &[u8]) -> Self {
//        HashValue::new(item)
//    }
//}
//impl AsRef<[u8]> for HashValue {
//    fn as_ref(&self) -> &[u8] {
//        &self.0[..]
//    }
//    }
//}

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
    let value = sodiumoxide::crypto::hash::sha256::hash(data).0;
    HashValue::new(&value)
}
