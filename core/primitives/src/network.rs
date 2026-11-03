use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
use near_schema_checker_lib::ProtocolSchema;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

/// Peer id is the public key.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct PeerId(Arc<PublicKey>);

impl PeerId {
    pub fn new(key: PublicKey) -> Self {
        Self(Arc::new(key))
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.0
    }
}

impl PeerId {
    #[cfg(feature = "rand")]
    pub fn random() -> Self {
        PeerId::new(near_crypto::SecretKey::from_random(near_crypto::KeyType::ED25519).public_key())
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
