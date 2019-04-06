extern crate exonum_sodiumoxide as sodiumoxide;

use std::convert::TryFrom;
use std::fmt;

use bs58;

use crate::logging::pretty_hash;
pub use crate::crypto::signature::sodiumoxide::crypto::sign::ed25519::Seed;
use crate::traits::Base58Encoded;
use crate::traits::ToBytes;
use crate::types::ReadablePublicKey;

#[derive(Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize)]
pub struct PublicKey(pub sodiumoxide::crypto::sign::ed25519::PublicKey);

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SecretKey(pub sodiumoxide::crypto::sign::ed25519::SecretKey);

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct Signature(pub sodiumoxide::crypto::sign::ed25519::Signature);

pub fn sign(data: &[u8], secret_key: &SecretKey) -> Signature {
    Signature(sodiumoxide::crypto::sign::ed25519::sign_detached(data, &secret_key.0))
}

pub fn verify(data: &[u8], signature: &Signature, public_key: &PublicKey) -> bool {
    sodiumoxide::crypto::sign::ed25519::verify_detached(&signature.0, data, &public_key.0)
}

pub fn get_key_pair() -> (PublicKey, SecretKey) {
    let (public_key, secret_key) = sodiumoxide::crypto::sign::ed25519::gen_keypair();
    (PublicKey(public_key), SecretKey(secret_key))
}

impl Base58Encoded for PublicKey {}
impl Base58Encoded for SecretKey {}
impl ToBytes for PublicKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_ref().to_vec()
    }
}

impl ToBytes for SecretKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_ref().to_vec()
    }
}

/// Allows using PublicKey in std containers and macros.
impl std::hash::Hash for PublicKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(self.as_ref());
    }
}

/// Allows using SecretKey in std containers and macros.
impl std::hash::Hash for SecretKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(self.as_ref());
    }
}

const SIG: [u8; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES] =
    [0u8; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES];

pub const DEFAULT_SIGNATURE: Signature =
    Signature(sodiumoxide::crypto::sign::ed25519::Signature(SIG));

impl PublicKey {
    pub fn to_readable(&self) -> ReadablePublicKey {
        ReadablePublicKey(self.to_string())
    }
}

impl TryFrom<&[u8]> for PublicKey {
    type Error = Box<std::error::Error>;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES {
            return Err("bytes not the size of a public key".into());
        }
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES];
        array.copy_from_slice(bytes);
        let public_key = sodiumoxide::crypto::sign::ed25519::PublicKey(array);
        Ok(PublicKey(public_key))
    }
}

impl TryFrom<&str> for PublicKey {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES];
        let bytes = bs58::decode(s)
            .into_vec()
            .map_err(|e| format!("Failed to convert public key from base58: {}", e))?;
        if bytes.len() != array.len() {
            return Err(format!("decoded {} is not long enough for public key", s));
        }
        let bytes_arr = &bytes[..array.len()];
        array.copy_from_slice(bytes_arr);
        let public_key = sodiumoxide::crypto::sign::ed25519::PublicKey(array);
        Ok(PublicKey(public_key))
    }
}

impl TryFrom<&[u8]> for SecretKey {
    type Error = Box<std::error::Error>;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != sodiumoxide::crypto::sign::ed25519::SECRETKEYBYTES {
            return Err("bytes not the size of a secret key".into());
        }
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::SECRETKEYBYTES];
        array.copy_from_slice(bytes);
        let secret_key = sodiumoxide::crypto::sign::ed25519::SecretKey(array);
        Ok(SecretKey(secret_key))
    }
}

impl TryFrom<&str> for SecretKey {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::SECRETKEYBYTES];
        let bytes = bs58::decode(s)
            .into_vec()
            .map_err(|e| format!("Failed to convert secret key from base58: {}", e))?;
        if bytes.len() != array.len() {
            return Err(format!("decoded {} is not long enough for secret key", s));
        }
        let bytes_arr = &bytes[..array.len()];
        array.copy_from_slice(bytes_arr);
        let secret_key = sodiumoxide::crypto::sign::ed25519::SecretKey(array);
        Ok(SecretKey(secret_key))
    }
}

impl TryFrom<&[u8]> for Signature {
    type Error = Box<std::error::Error>;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES {
            return Err("bytes not the size of a signature".into());
        }
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES];
        array.copy_from_slice(bytes);
        let signature = sodiumoxide::crypto::sign::ed25519::Signature(array);
        Ok(Signature(signature))
    }
}

impl TryFrom<&str> for Signature {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES];
        let bytes = bs58::decode(s)
            .into_vec()
            .map_err(|e| format!("Failed to convert signature from base58: {}", e))?;
        if bytes.len() != array.len() {
            return Err(format!("decoded {} is not long enough for signature", s));
        }
        let bytes_arr = &bytes[..array.len()];
        array.copy_from_slice(bytes_arr);
        let signature = sodiumoxide::crypto::sign::ed25519::Signature(array);
        Ok(Signature(signature))
    }
}

impl std::convert::AsRef<[u8]> for PublicKey {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

impl<'a> From<&'a PublicKey> for String {
    fn from(h: &'a PublicKey) -> Self {
        bs58::encode(h.0).into_string()
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", pretty_hash(&String::from(self)))
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

impl std::convert::AsRef<[u8]> for SecretKey {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

impl<'a> From<&'a SecretKey> for String {
    fn from(h: &'a SecretKey) -> Self {
        bs58::encode(h).into_string()
    }
}

impl std::convert::AsRef<[u8]> for Signature {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", pretty_hash(&String::from(self)))
    }
}

impl fmt::Display for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

impl<'a> From<&'a Signature> for String {
    fn from(h: &'a Signature) -> Self {
        bs58::encode(h).into_string()
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", pretty_hash(&String::from(self)))
    }
}

impl fmt::Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

pub mod bs58_pub_key_format {
    use serde::{de::Error, Deserialize, Deserializer, Serializer};

    use super::PublicKey;
    use std::convert::TryInto;

    pub fn serialize<S>(public_key: &PublicKey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(String::from(public_key).as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PublicKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?.as_str().try_into().map_err(Error::custom)
    }
}

pub mod bs58_secret_key_format {
    use serde::{de::Error, Deserialize, Deserializer, Serializer};

    use super::SecretKey;
    use std::convert::TryInto;

    pub fn serialize<S>(secret_key: &SecretKey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(String::from(secret_key).as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SecretKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?.as_str().try_into().map_err(Error::custom)
    }
}

pub mod bs58_signature_format {
    use serde::{de::Error, Deserialize, Deserializer, Serializer};

    use super::Signature;
    use std::convert::TryInto;

    pub fn serialize<S>(signature: &Signature, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(String::from(signature).as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Signature, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?.as_str().try_into().map_err(Error::custom)
    }
}

pub mod bs58_serializer {
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::traits::Base58Encoded;

    pub fn serialize<T, S>(t: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Base58Encoded,
        S: Serializer,
    {
        serializer.serialize_str(&t.to_base58())
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: Base58Encoded,
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(T::from_base58(&s).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify() {
        let (public_key, private_key) = get_key_pair();
        let data = b"123";
        let signature = sign(data, &private_key);
        assert!(verify(data, &signature, &public_key));
    }
}
