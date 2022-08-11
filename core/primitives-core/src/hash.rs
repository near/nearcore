use std::fmt;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::Digest;

use crate::borsh::BorshSerialize;
use crate::logging::pretty_hash;
use crate::serialize::from_base;

#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, derive_more::AsRef, derive_more::AsMut)]
#[as_ref(forward)]
#[as_mut(forward)]
pub struct CryptoHash(pub [u8; 32]);

impl CryptoHash {
    pub const fn new() -> Self {
        Self([0; 32])
    }

    pub fn hash_bytes(bytes: &[u8]) -> CryptoHash {
        CryptoHash(sha2::Sha256::digest(bytes).into())
    }

    pub fn hash_borsh<T: BorshSerialize>(value: &T) -> CryptoHash {
        let mut hasher = sha2::Sha256::default();
        BorshSerialize::serialize(value, &mut hasher).unwrap();
        CryptoHash(hasher.finalize().into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Converts hash into base58-encoded string and passes it to given visitor.
    ///
    /// The conversion is performed without any memory allocation.  The visitor
    /// is given a reference to a string stored on stack.  Returns whatever the
    /// visitor returns.
    fn to_base58_impl<Out>(&self, visitor: impl FnOnce(&str) -> Out) -> Out {
        // base58-encoded string is at most 1.4 times longer than the binary
        // sequence.  We’re serialising 32 bytes so ⌈32 * 1.4⌉ = 45 should be
        // enough.
        let mut buffer = [0u8; 45];
        let len = bs58::encode(self).into(&mut buffer[..]).unwrap();
        let value = std::str::from_utf8(&buffer[..len]).unwrap();
        visitor(value)
    }
}

impl Default for CryptoHash {
    fn default() -> Self {
        Self::new()
    }
}

impl borsh::BorshSerialize for CryptoHash {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> Result<(), std::io::Error> {
        writer.write_all(&self.0)?;
        Ok(())
    }
}

impl borsh::BorshDeserialize for CryptoHash {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, std::io::Error> {
        Ok(CryptoHash(borsh::BorshDeserialize::deserialize(buf)?))
    }
}

impl Serialize for CryptoHash {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        self.to_base58_impl(|encoded| serializer.serialize_str(encoded))
    }
}

impl<'de> Deserialize<'de> for CryptoHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // base58-encoded string is at most 1.4 longer than the binary sequence, but factor of 2 is
        // good enough to prevent DoS.
        if s.len() > std::mem::size_of::<CryptoHash>() * 2 {
            return Err(serde::de::Error::custom("incorrect length for hash"));
        }
        from_base(&s)
            .and_then(|f| CryptoHash::try_from(f.as_slice()))
            .map_err(|err| serde::de::Error::custom(err.to_string()))
    }
}

impl std::str::FromStr for CryptoHash {
    type Err = Box<dyn std::error::Error + Send + Sync>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = from_base(s).map_err::<Self::Err, _>(|e| e.to_string().into())?;
        Self::try_from(bytes.as_slice())
    }
}

impl TryFrom<&[u8]> for CryptoHash {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Ok(CryptoHash(bytes.try_into()?))
    }
}

impl From<CryptoHash> for Vec<u8> {
    fn from(hash: CryptoHash) -> Vec<u8> {
        hash.0.to_vec()
    }
}

impl From<&CryptoHash> for Vec<u8> {
    fn from(hash: &CryptoHash) -> Vec<u8> {
        hash.0.to_vec()
    }
}

impl From<CryptoHash> for [u8; 32] {
    fn from(hash: CryptoHash) -> [u8; 32] {
        hash.0
    }
}

impl fmt::Debug for CryptoHash {
    fn fmt(&self, fmtr: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_base58_impl(|encoded| write!(fmtr, "{}", pretty_hash(encoded)))
    }
}

impl fmt::Display for CryptoHash {
    fn fmt(&self, fmtr: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.to_base58_impl(|encoded| fmtr.write_str(encoded))
    }
}

impl Hash for CryptoHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.as_ref());
    }
}

/// Calculates a hash of a bytes slice.
///
/// # Examples
///
/// The example below calculates the hash of the indicated data.
///
/// ```
/// let data = [1, 2, 3];
/// let hash = near_primitives_core::hash::hash(&data);
/// ```
pub fn hash(data: &[u8]) -> CryptoHash {
    CryptoHash::hash_bytes(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::str::FromStr;

    #[derive(Deserialize, Serialize)]
    struct Struct {
        hash: CryptoHash,
    }

    #[test]
    fn test_base58_successes() {
        for (encoded, hash) in [
            ("11111111111111111111111111111111", CryptoHash::new()),
            ("CjNSmWXTWhC3EhRVtqLhRmWMTkRbU96wUACqxMtV1uGf", hash(&[0, 1, 2])),
        ] {
            assert_eq!(encoded, hash.to_string());
            assert_eq!(hash, CryptoHash::from_str(encoded).unwrap());

            let json = format!("\"{}\"", encoded);
            assert_eq!(json, serde_json::to_string(&hash).unwrap());
            assert_eq!(hash, serde_json::from_str::<CryptoHash>(&json).unwrap());
        }
    }

    #[test]
    fn test_deserialize_not_base58() {
        let encoded = "\"---\"";
        match serde_json::from_str(encoded) {
            Ok(CryptoHash(_)) => assert!(false, "should have failed"),
            Err(_) => (),
        }
    }

    #[test]
    fn test_deserialize_not_crypto_hash() {
        for encoded in &[
            "\"CjNSmWXTWhC3ELhRmWMTkRbU96wUACqxMtV1uGf\"".to_string(),
            "\"\"".to_string(),
            format!("\"{}\"", "1".repeat(31)),
            format!("\"{}\"", "1".repeat(33)),
            format!("\"{}\"", "1".repeat(1000)),
        ] {
            match serde_json::from_str::<CryptoHash>(encoded) {
                Err(e) => if e.to_string() == "could not convert slice to array" {},
                res => assert!(false, "should have failed with incorrect length error: {:?}", res),
            };
        }
    }
}
