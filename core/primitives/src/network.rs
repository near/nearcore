use std::fmt;
use std::hash::Hash;

use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use near_crypto::{KeyType, PublicKey, SecretKey, Signature};

use crate::hash::{hash, CryptoHash};
use crate::types::{AccountId, EpochId};

/// Peer id is the public key.
#[derive(
    BorshSerialize, BorshDeserialize, Clone, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash,
)]
pub struct PeerId(pub PublicKey);

impl PeerId {
    pub fn new(key: PublicKey) -> Self {
        Self(key)
    }

    pub fn public_key(&self) -> PublicKey {
        self.0.clone()
    }
}

impl PeerId {
    pub fn random() -> Self {
        SecretKey::from_random(KeyType::ED25519).public_key().into()
    }
}

impl From<PeerId> for Vec<u8> {
    fn from(peer_id: PeerId) -> Vec<u8> {
        peer_id.0.try_to_vec().unwrap()
    }
}

impl From<PublicKey> for PeerId {
    fn from(public_key: PublicKey) -> PeerId {
        PeerId(public_key)
    }
}

impl TryFrom<Vec<u8>> for PeerId {
    type Error = Box<dyn std::error::Error>;

    fn try_from(bytes: Vec<u8>) -> Result<PeerId, Self::Error> {
        Ok(PeerId(PublicKey::try_from_slice(&bytes)?))
    }
}

impl PartialEq for PeerId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
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

/// Account announcement information
#[derive(BorshSerialize, BorshDeserialize, Serialize, PartialEq, Eq, Clone, Debug)]
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
        let header = AnnounceAccountRouteHeader {
            account_id: account_id.clone(),
            peer_id: peer_id.clone(),
            epoch_id: epoch_id.clone(),
        };
        hash(&header.try_to_vec().unwrap())
    }

    pub fn hash(&self) -> CryptoHash {
        AnnounceAccount::build_header_hash(&self.account_id, &self.peer_id, &self.epoch_id)
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
struct AnnounceAccountRouteHeader {
    pub account_id: AccountId,
    pub peer_id: PeerId,
    pub epoch_id: EpochId,
}
