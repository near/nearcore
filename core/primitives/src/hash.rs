use bs58;
use exonum_sodiumoxide as sodiumoxide;
use exonum_sodiumoxide::crypto::hash::sha256::Digest;
use heapsize;
use std::fmt;
use traits::Encode;

#[derive(Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize, Hash)]
pub struct CryptoHash(pub Digest);

impl CryptoHash {
    pub fn new(data: &[u8]) -> Self {
        let mut d = [0; 32];
        d.copy_from_slice(data);
        CryptoHash(Digest(d))
    }
}

impl<'a> From<&'a CryptoHash> for String {
    fn from(h: &'a CryptoHash) -> Self {
        bs58::encode(h.0).into_string()
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
        write!(f, "{}", String::from(self))
    }
}

impl fmt::Display for CryptoHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

pub mod bs58_format {
    use super::{bs58, CryptoHash};
    use serde::{Deserialize, Serializer, Deserializer};
    use serde::de;

    pub fn serialize<S>(
        crypto_hash: &CryptoHash,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        serializer.serialize_str(String::from(crypto_hash).as_str())
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<CryptoHash, D::Error>
        where
            D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match bs58::decode(s).into_vec() {
            Ok(vec) => {
                let mut array = [0; 32];
                if vec.len() == array.len() {
                    let bytes = &vec[..array.len()];
                    Ok(CryptoHash::new(bytes))
                } else {
                    Err(de::Error::custom("invalid byte array length"))
                }
            }
            Err(_) => Err(de::Error::custom("invalid base58 string")),
        }
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

#[cfg(test)]
mod tests {
    extern crate serde_json;

    use super::*;

    #[derive(Deserialize, Serialize)]
    struct Struct {
        #[serde(with = "bs58_format")]
        hash: CryptoHash,
    }

    #[test]
    fn test_serialize_success() {
        let hash = hash(&[0, 1, 2]);
        let s = Struct { hash };
        let encoded = serde_json::to_string(&s).unwrap();
        assert_eq!(encoded, "{\"hash\":\"CjNSmWXTWhC3EhRVtqLhRmWMTkRbU96wUACqxMtV1uGf\"}");
    }

    #[test]
    fn test_deserialize_success() {
        let encoded = "{\"hash\":\"CjNSmWXTWhC3EhRVtqLhRmWMTkRbU96wUACqxMtV1uGf\"}";
        let decoded: Struct = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.hash, hash(&[0, 1, 2]));
    }

    #[test]
    fn test_deserialize_not_base58() {
        let encoded = "\"---\"";
        match serde_json::from_str(&encoded) {
            Ok(CryptoHash(_)) => assert!(false, "should have failed"),
            Err(_) => (),
        }
    }

    #[test]
    fn test_deserialize_not_crypto_hash() {
        let encoded = "\"CjNSmWXTWhC3ELhRmWMTkRbU96wUACqxMtV1uGf\"";
        match serde_json::from_str(&encoded) {
            Ok(CryptoHash(_)) => assert!(false, "should have failed"),
            Err(_) => (),
        }
    }
}
