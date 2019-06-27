extern crate exonum_sodiumoxide as sodiumoxide;

use std::convert::TryFrom;
use std::fmt;
use std::hash::{Hash, Hasher};

pub use exonum_sodiumoxide::crypto::sign::ed25519::Seed;

use crate::logging::pretty_hash;
use crate::serialize::{from_base, to_base, BaseDecode};
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

impl From<&PublicKey> for Vec<u8> {
    fn from(public_key: &PublicKey) -> Self {
        public_key.as_ref().to_vec()
    }
}

impl From<&SecretKey> for Vec<u8> {
    fn from(secret_key: &SecretKey) -> Self {
        secret_key.as_ref().to_vec()
    }
}

const SIG: [u8; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES] =
    [0u8; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES];

pub const DEFAULT_SIGNATURE: Signature =
    Signature(sodiumoxide::crypto::sign::ed25519::Signature(SIG));

impl BaseDecode for PublicKey {}
impl BaseDecode for SecretKey {}

impl PublicKey {
    pub fn to_readable(&self) -> ReadablePublicKey {
        ReadablePublicKey(self.to_string())
    }
    pub fn empty() -> Self {
        let array = [0; sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES];
        let public_key = sodiumoxide::crypto::sign::ed25519::PublicKey(array);
        PublicKey(public_key)
    }
}

impl Hash for PublicKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.as_ref());
    }
}

impl TryFrom<&[u8]> for PublicKey {
    type Error = Box<dyn std::error::Error>;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES {
            return Err(format!(
                "bytes not the size {} of a public key {}: {:?}",
                bytes.len(),
                sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES,
                bytes
            )
            .into());
        }
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES];
        array.copy_from_slice(bytes);
        let public_key = sodiumoxide::crypto::sign::ed25519::PublicKey(array);
        Ok(PublicKey(public_key))
    }
}

impl TryFrom<Vec<u8>> for PublicKey {
    type Error = Box<dyn std::error::Error>;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let bytes: &[u8] = bytes.as_ref();
        Self::try_from(bytes)
    }
}

impl TryFrom<&str> for PublicKey {
    type Error = Box<dyn std::error::Error>;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::PUBLICKEYBYTES];
        let bytes = from_base(s).map_err::<Self::Error, _>(|e| {
            format!("Failed to convert public key from base58: {}", e).into()
        })?;
        if bytes.len() != array.len() {
            return Err(format!("decoded {} is not long enough for public key", s).into());
        }
        let bytes_arr = &bytes[..array.len()];
        array.copy_from_slice(bytes_arr);
        let public_key = sodiumoxide::crypto::sign::ed25519::PublicKey(array);
        Ok(PublicKey(public_key))
    }
}

impl TryFrom<&[u8]> for SecretKey {
    type Error = Box<dyn std::error::Error>;

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
    type Error = Box<dyn std::error::Error>;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::SECRETKEYBYTES];
        let bytes = from_base(s).map_err::<Self::Error, _>(|e| {
            format!("Failed to convert secret key from base58: {}", e).into()
        })?;
        if bytes.len() != array.len() {
            return Err(format!("decoded {} is not long enough for secret key", s).into());
        }
        let bytes_arr = &bytes[..array.len()];
        array.copy_from_slice(bytes_arr);
        let secret_key = sodiumoxide::crypto::sign::ed25519::SecretKey(array);
        Ok(SecretKey(secret_key))
    }
}

impl TryFrom<&[u8]> for Signature {
    type Error = Box<dyn std::error::Error>;

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

impl TryFrom<Vec<u8>> for Signature {
    type Error = Box<dyn std::error::Error>;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let bytes: &[u8] = bytes.as_ref();
        Self::try_from(bytes)
    }
}

impl TryFrom<&str> for Signature {
    type Error = Box<dyn std::error::Error>;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let mut array = [0; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES];
        let bytes = from_base(s).map_err::<Self::Error, _>(|e| {
            format!("Failed to convert signature from base58: {}", e).into()
        })?;
        if bytes.len() != array.len() {
            return Err(format!("decoded {} is not long enough for signature", s).into());
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
        to_base(&h.0)
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
        to_base(h)
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
        to_base(h)
    }
}

impl<'a> From<&'a Signature> for Vec<u8> {
    fn from(h: &'a Signature) -> Self {
        (h.0).0.to_vec()
    }
}

impl From<Signature> for Vec<u8> {
    fn from(h: Signature) -> Self {
        (h.0).0.to_vec()
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
