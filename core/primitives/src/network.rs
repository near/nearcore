use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use near_crypto::{KeyType, PublicKey, SecretKey, Signature};

use crate::hash::CryptoHash;
use crate::types::{AccountId, EpochId};

/// Peer id is the public key.
#[derive(
    BorshSerialize, BorshDeserialize, Clone, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct PeerId(Arc<PeerIdInner>);

/// Peer id is the public key.
#[derive(
    BorshSerialize, BorshDeserialize, Clone, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash,
)]
pub struct PeerIdInner(PublicKey);

impl PeerId {
    pub fn new(key: PublicKey) -> Self {
        Self(Arc::new(PeerIdInner(key)))
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.0 .0
    }
}

impl PeerIdInner {
    pub fn new(key: PublicKey) -> Self {
        Self(key)
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.0
    }
}

impl PeerId {
    pub fn random() -> Self {
        PeerId::new(SecretKey::from_random(KeyType::ED25519).public_key())
    }
}

impl TryFrom<Vec<u8>> for PeerId {
    type Error = Box<dyn std::error::Error>;

    fn try_from(bytes: Vec<u8>) -> Result<PeerId, Self::Error> {
        Ok(PeerId::new(PublicKey::try_from_slice(&bytes)?))
    }
}

impl PartialEq for PeerId {
    fn eq(&self, other: &Self) -> bool {
        self.0 .0 == other.0 .0
    }
}

impl PartialEq for PeerIdInner {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0 .0)
    }
}

impl fmt::Debug for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0 .0)
    }
}

/// Account announcement information
#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
pub struct AnnounceAccount {
    /// AccountId to be announced.
    pub account_id: AccountId,
    /// PeerId from the owner of the account.
    pub peer_id: PeerId,
    /// This announcement is only valid for this `epoch`.
    pub epoch_id: EpochId,
    /// Signature using AccountId associated secret key.
    pub signature: Signature,
}

impl AnnounceAccount {
    pub fn build_header_hash(
        account_id: &AccountId,
        peer_id: &PeerId,
        epoch_id: &EpochId,
    ) -> CryptoHash {
        let header = AnnounceAccountRouteHeader { account_id, peer_id, epoch_id };

        CryptoHash::hash_borsh(&header)
    }

    pub fn hash(&self) -> CryptoHash {
        AnnounceAccount::build_header_hash(&self.account_id, &self.peer_id, &self.epoch_id)
    }
}

#[derive(BorshSerialize)]
struct AnnounceAccountRouteHeader<'a> {
    pub account_id: &'a AccountId,
    pub peer_id: &'a PeerId,
    pub epoch_id: &'a EpochId,
}
