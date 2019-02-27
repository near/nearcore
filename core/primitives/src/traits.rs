use crate::hash::CryptoHash;

use super::aggregate_signature;
use super::types;

/// Trait to abstract the way signing happens.
/// Can be used to not keep private key in the given binary via cross-process communication.
pub trait Signer: Sync + Send {
    fn public_key(&self) -> aggregate_signature::BlsPublicKey;
    fn sign(&self, hash: &CryptoHash) -> types::PartialSignature;
    fn account_id(&self) -> types::AccountId;
}

/// FromBytes is like TryFrom<Vec<u8>>
pub trait FromBytes: Sized {
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, Box<std::error::Error>>;
}

/// ToBytes is like Into<Vec<u8>>, but doesn't consume self
pub trait ToBytes: Sized {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait Base58Encoded : FromBytes + ToBytes {
    fn from_base58(s: &String) -> Result<Self, Box<std::error::Error>> {
        let bytes = bs58::decode(s).into_vec()?;
        Self::from_bytes(bytes)
    }

    fn to_base58(&self) -> String {
        let bytes = self.to_bytes();
        bs58::encode(bytes).into_string()
    }
}
