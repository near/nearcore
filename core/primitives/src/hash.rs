use std::convert::TryFrom;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::Read;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::logging::pretty_hash;
use crate::serialize::{from_base, to_base, BaseDecode};

/// Length of CryptoHash.
pub const CRYPTO_HASH_LEN: usize = 32;

#[derive(Copy, Clone, PartialOrd, PartialEq, Eq, Ord, derive_more::AsRef)]
#[as_ref(forward)]
pub struct Digest(pub [u8; CRYPTO_HASH_LEN]);

#[derive(Copy, Clone, PartialOrd, Ord, derive_more::AsRef)]
#[as_ref(forward)]
pub struct CryptoHash(pub Digest);

impl<'a> From<&'a CryptoHash> for String {
    fn from(h: &'a CryptoHash) -> Self {
        to_base(&h.0)
    }
}

impl Default for CryptoHash {
    fn default() -> Self {
        CryptoHash(Digest(Default::default()))
    }
}

impl AsMut<[u8]> for CryptoHash {
    fn as_mut(&mut self) -> &mut [u8] {
        (self.0).0.as_mut()
    }
}

impl BaseDecode for CryptoHash {}

impl borsh::BorshSerialize for CryptoHash {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> Result<(), std::io::Error> {
        writer.write_all(&(self.0).0)?;
        Ok(())
    }
}

impl borsh::BorshDeserialize for CryptoHash {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut bytes = [0; 32];
        reader.read_exact(&mut bytes)?;
        Ok(CryptoHash(Digest(bytes)))
    }
}

impl Serialize for CryptoHash {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&to_base(&self.0))
    }
}

impl<'de> Deserialize<'de> for CryptoHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        from_base(&s)
            .and_then(CryptoHash::try_from)
            .map_err(|err| serde::de::Error::custom(err.to_string()))
    }
}

impl TryFrom<&str> for CryptoHash {
    type Error = Box<dyn std::error::Error>;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let bytes = from_base(s).map_err::<Self::Error, _>(|e| format!("{}", e).into())?;
        Self::try_from(bytes)
    }
}

impl TryFrom<String> for CryptoHash {
    type Error = Box<dyn std::error::Error>;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        <Self as TryFrom<&str>>::try_from(&s.as_str())
    }
}

impl TryFrom<&[u8]> for CryptoHash {
    type Error = Box<dyn std::error::Error>;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != 32 {
            return Err("incorrect length for hash".into());
        }
        let mut buf = [0; 32];
        buf.copy_from_slice(bytes);
        Ok(CryptoHash(Digest(buf)))
    }
}

impl TryFrom<Vec<u8>> for CryptoHash {
    type Error = Box<dyn std::error::Error>;

    fn try_from(v: Vec<u8>) -> Result<Self, Self::Error> {
        <Self as TryFrom<&[u8]>>::try_from(v.as_ref())
    }
}

impl From<CryptoHash> for Vec<u8> {
    fn from(hash: CryptoHash) -> Vec<u8> {
        (hash.0).0.to_vec()
    }
}

impl From<&CryptoHash> for Vec<u8> {
    fn from(hash: &CryptoHash) -> Vec<u8> {
        (hash.0).0.to_vec()
    }
}

impl fmt::Debug for CryptoHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", pretty_hash(&String::from(self)))
    }
}

impl fmt::Display for CryptoHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

impl Hash for CryptoHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.as_ref());
    }
}

impl PartialEq for CryptoHash {
    fn eq(&self, other: &CryptoHash) -> bool {
        self.0 == other.0
    }
}

impl Eq for CryptoHash {}

/// Calculates a hash of a bytes slice.
///
/// # Examples
///
/// The example below calculates the hash of the indicated data.
///
/// ```
/// let data = [1, 2, 3];
/// let hash = near_primitives::hash::hash(&data);
/// ```
pub fn hash(data: &[u8]) -> CryptoHash {
    use sha2::Digest;
    CryptoHash(Digest(sha2::Sha256::digest(data).into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize, Serialize)]
    struct Struct {
        hash: CryptoHash,
    }

    #[test]
    fn test_serialize_success() {
        let hash = hash(&[0, 1, 2]);
        let s = Struct { hash: hash.into() };
        let encoded = serde_json::to_string(&s).unwrap();
        assert_eq!(encoded, "{\"hash\":\"CjNSmWXTWhC3EhRVtqLhRmWMTkRbU96wUACqxMtV1uGf\"}");
    }

    #[test]
    fn test_serialize_default() {
        let s = Struct { hash: CryptoHash::default().into() };
        let encoded = serde_json::to_string(&s).unwrap();
        assert_eq!(encoded, "{\"hash\":\"11111111111111111111111111111111\"}");
    }

    #[test]
    fn test_deserialize_default() {
        let encoded = "{\"hash\":\"11111111111111111111111111111111\"}";
        let decoded: Struct = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.hash, CryptoHash::default().into());
    }

    #[test]
    fn test_deserialize_success() {
        let encoded = "{\"hash\":\"CjNSmWXTWhC3EhRVtqLhRmWMTkRbU96wUACqxMtV1uGf\"}";
        let decoded: Struct = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.hash, hash(&[0, 1, 2]).into());
    }

    #[test]
    fn test_deserialize_not_base64() {
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
