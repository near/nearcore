extern crate exonum_sodiumoxide as sodiumoxide;

use bs58;
use crate::hash;
use std::fmt;

use crate::logging::pretty_hash;
use crate::types::ReadablePublicKey;
pub use crate::signature::sodiumoxide::crypto::sign::ed25519::Seed;

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

pub fn verify_signature(signature: &Signature, hash: &hash::CryptoHash, pubkey: &PublicKey) -> bool {
    sodiumoxide::crypto::sign::ed25519::verify_detached(&signature.0, hash.as_ref(), &pubkey.0)
}

const SIG: [u8; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES] =
    [0u8; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES];

pub const DEFAULT_SIGNATURE: Signature = Signature(sodiumoxide::crypto::sign::ed25519::Signature(SIG));

impl PublicKey {
    pub fn new(bytes: &[u8]) -> Result<PublicKey, String> {
        if bytes.len() != sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES {
            return Err("bytes not the size of a public key".to_string())
        }
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES];
        array.copy_from_slice(bytes);
        let public_key = sodiumoxide::crypto::sign::ed25519::PublicKey(array);
        Ok(PublicKey(public_key))
    }

    pub fn from(s: &str) -> PublicKey {
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES];
        let bytes = bs58::decode(s).into_vec().expect("Failed to convert public key from base58");
        assert_eq!(bytes.len(), array.len(), "decoded {} is not long enough for public key", s);
        let bytes_arr = &bytes[..array.len()];
        array.copy_from_slice(bytes_arr);
        let public_key = sodiumoxide::crypto::sign::ed25519::PublicKey(array);
        PublicKey(public_key)
    }

    pub fn to_readable(&self) -> ReadablePublicKey {
        ReadablePublicKey(self.to_string())
    }
}

impl SecretKey {
    pub fn from(s: &str) -> SecretKey {
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::SECRETKEYBYTES];
        let bytes = bs58::decode(s).into_vec().expect("Failed to convert secret key from base58");
        assert_eq!(bytes.len(), array.len(), "decoded {} is not long enough for secret key", s);
        let bytes_arr = &bytes[..array.len()];
        array.copy_from_slice(bytes_arr);
        let secret_key = sodiumoxide::crypto::sign::ed25519::SecretKey(array);
        SecretKey(secret_key)
    }
}

impl Signature {
    pub fn new(bytes: &[u8]) -> Signature {
        assert!(bytes.len() == sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES);
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES];
        array.copy_from_slice(bytes);
        let signature = sodiumoxide::crypto::sign::ed25519::Signature(array);
        Signature(signature)
    }

    pub fn from(s: &str) -> Signature {
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES];
        let bytes = bs58::decode(s).into_vec().expect("Failed to convert signature from base58");
        assert_eq!(bytes.len(), array.len(), "decoded {} is not long enough for signature", s);
        let bytes_arr = &bytes[..array.len()];
        array.copy_from_slice(bytes_arr);
        let signature = sodiumoxide::crypto::sign::ed25519::Signature(array);
        Signature(signature)
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
    use super::PublicKey;
    use serde::{Deserialize, Deserializer, Serializer};

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
        let s = String::deserialize(deserializer)?;
        Ok(PublicKey::from(&s))
    }
}

pub mod bs58_secret_key_format {
    use super::SecretKey;
    use serde::{Deserialize, Deserializer, Serializer};

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
        let s = String::deserialize(deserializer)?;
        Ok(SecretKey::from(&s))
    }
}

pub mod bs58_signature_format {
    use super::Signature;
    use serde::{Deserialize, Deserializer, Serializer};

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
        let s = String::deserialize(deserializer)?;
        Ok(Signature::from(&s))
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
