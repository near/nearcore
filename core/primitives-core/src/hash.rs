use std::convert::TryFrom;
use std::fmt;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::logging::pretty_hash;
use crate::serialize::{from_base, to_base, BaseDecode};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, derive_more::AsRef, derive_more::AsMut)]
#[as_ref(forward)]
#[as_mut(forward)]
pub struct CryptoHash(pub [u8; 32]);

impl Default for CryptoHash {
    fn default() -> Self {
        CryptoHash(Default::default())
    }
}

impl BaseDecode for CryptoHash {}

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
        serializer.serialize_str(&to_base(&self.0))
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
            .and_then(CryptoHash::try_from)
            .map_err(|err| serde::de::Error::custom(err.to_string()))
    }
}

impl std::str::FromStr for CryptoHash {
    type Err = Box<dyn std::error::Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = from_base(s).map_err::<Self::Err, _>(|e| e.to_string().into())?;
        Self::try_from(bytes)
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
        Ok(CryptoHash(buf))
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", pretty_hash(&self.to_string()))
    }
}

impl fmt::Display for CryptoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&to_base(&self.0), f)
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
    use sha2::Digest;
    CryptoHash(sha2::Sha256::digest(data).into())
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
    fn test_deserialize_not_base58() {
        let encoded = "\"---\"";
        match serde_json::from_str(&encoded) {
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
            match serde_json::from_str::<CryptoHash>(&encoded) {
                Err(e) if e.to_string() == "incorrect length for hash" => {}
                res => assert!(false, "should have failed with incorrect length error: {:?}", res),
            };
        }
    }
}
