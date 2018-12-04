extern crate exonum_sodiumoxide as sodiumoxide;

use bs58;
use std::fmt;

pub use signature::sodiumoxide::crypto::sign::ed25519::Seed;

#[derive(Copy, Clone, Eq, PartialOrd, Ord, PartialEq, Serialize, Deserialize)]
pub struct PublicKey(pub sodiumoxide::crypto::sign::ed25519::PublicKey);

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SecretKey(pub sodiumoxide::crypto::sign::ed25519::SecretKey);

pub type Signature = sodiumoxide::crypto::sign::ed25519::Signature;

pub fn sign(data: &[u8], secret_key: &SecretKey) -> Signature {
    sodiumoxide::crypto::sign::ed25519::sign_detached(data, &secret_key.0)
}

pub fn get_keypair_from_seed(seed: &Seed) -> (PublicKey, SecretKey) {
    let (public_key, secret_key) = sodiumoxide::crypto::sign::ed25519::keypair_from_seed(seed);
    (PublicKey(public_key), SecretKey(secret_key))
}

pub fn get_keypair() -> (PublicKey, SecretKey) {
    let (public_key, secret_key) = sodiumoxide::crypto::sign::ed25519::gen_keypair();
    (PublicKey(public_key), SecretKey(secret_key))
}


const SIG: [u8; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES] = [
    0u8;
    sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES
];

pub const DEFAULT_SIGNATURE: Signature = sodiumoxide::crypto::sign::ed25519::Signature(SIG);

impl PublicKey {
    pub fn from(s: &str) -> PublicKey {
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES];
        let bytes = bs58::decode(s).into_vec().expect("Failed to convert public key from base58");
        assert_eq!(bytes.len(), array.len(), "decoded {} is not long enough for public key", s);
        let bytes_arr = &bytes[..array.len()];
        array.copy_from_slice(bytes_arr);
        let public_key = sodiumoxide::crypto::sign::ed25519::PublicKey(array);
        PublicKey(public_key)
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

impl<'a> From<&'a PublicKey> for String {
    fn from(h: &'a PublicKey) -> Self {
        bs58::encode(h.0).into_string()
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", String::from(self))
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

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

impl fmt::Display for SecretKey {
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
