/// Conversion functions for messages representing crypto primitives.
use crate::network_protocol::proto;
use borsh::{BorshDeserialize as _, BorshSerialize as _};
use near_crypto::PublicKey;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;

//////////////////////////////////////////

pub type ParseCryptoHashError = Box<dyn std::error::Error + Send + Sync>;

impl From<&CryptoHash> for proto::CryptoHash {
    fn from(x: &CryptoHash) -> Self {
        let mut y = Self::new();
        y.hash = x.0.into();
        y
    }
}

impl TryFrom<&proto::CryptoHash> for CryptoHash {
    type Error = ParseCryptoHashError;
    fn try_from(p: &proto::CryptoHash) -> Result<Self, Self::Error> {
        CryptoHash::try_from(&p.hash[..])
    }
}

//////////////////////////////////////////

pub type ParsePublicKeyError = borsh::maybestd::io::Error;

impl From<&PublicKey> for proto::PublicKey {
    fn from(x: &PublicKey) -> Self {
        Self { borsh: x.try_to_vec().unwrap(), ..Self::default() }
    }
}

impl TryFrom<&proto::PublicKey> for PublicKey {
    type Error = ParsePublicKeyError;
    fn try_from(p: &proto::PublicKey) -> Result<Self, Self::Error> {
        Self::try_from_slice(&p.borsh)
    }
}

impl From<&PeerId> for proto::PublicKey {
    fn from(x: &PeerId) -> Self {
        x.public_key().into()
    }
}

impl TryFrom<&proto::PublicKey> for PeerId {
    type Error = ParsePublicKeyError;
    fn try_from(p: &proto::PublicKey) -> Result<Self, Self::Error> {
        Ok(PeerId::new(PublicKey::try_from(p)?))
    }
}

//////////////////////////////////////////

pub type ParseSignatureError = borsh::maybestd::io::Error;

impl From<&near_crypto::Signature> for proto::Signature {
    fn from(x: &near_crypto::Signature) -> Self {
        Self { borsh: x.try_to_vec().unwrap(), ..Self::default() }
    }
}

impl TryFrom<&proto::Signature> for near_crypto::Signature {
    type Error = ParseSignatureError;
    fn try_from(x: &proto::Signature) -> Result<Self, Self::Error> {
        Self::try_from_slice(&x.borsh)
    }
}
