/// Conversion functions for messages representing crypto primitives.
use crate::network_protocol::proto;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{PeerId};
use borsh::{BorshDeserialize as _, BorshSerialize as _};

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

pub type ParsePeerIdError = borsh::maybestd::io::Error;

impl From<&PeerId> for proto::PublicKey {
    fn from(x: &PeerId) -> Self {
        Self { borsh: x.try_to_vec().unwrap(), ..Self::default() }
    }
}

impl TryFrom<&proto::PublicKey> for PeerId {
    type Error = ParsePeerIdError;
    fn try_from(p: &proto::PublicKey) -> Result<Self, Self::Error> {
        Self::try_from_slice(&p.borsh)
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
