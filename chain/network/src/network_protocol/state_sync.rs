use super::MAX_SHARDS_PER_SNAPSHOT_HOST_INFO;
use crate::network_protocol::Arc;
use near_crypto::SecretKey;
use near_crypto::Signature;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::EpochHeight;
use near_primitives::types::ShardId;
use near_schema_checker_lib::ProtocolSchema;
// TODO(saketh): Consider moving other types related to state sync into this file
// e.g. StateResponseInfo

/// Specifies information about a state snapshot hosted by a network peer.
///
/// A signature is included so that we know it was really published by that peer.
///
#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Hash,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    ProtocolSchema,
)]
pub struct SnapshotHostInfo {
    /// Id of the node serving the snapshot
    pub peer_id: PeerId,
    /// Hash of the snapshot's state root
    pub sync_hash: CryptoHash,
    /// Ordinal of the epoch of the state root
    pub epoch_height: EpochHeight,
    /// List of shards included in the snapshot.
    pub shards: Vec<ShardId>,
    /// Signature on (sync_hash, epoch_height, shards)
    pub signature: Signature,
}

impl SnapshotHostInfo {
    fn build_hash(
        sync_hash: &CryptoHash,
        epoch_height: &EpochHeight,
        shards: &Vec<ShardId>,
    ) -> CryptoHash {
        CryptoHash::hash_borsh((sync_hash, epoch_height, shards))
    }

    pub(crate) fn new(
        peer_id: PeerId,
        sync_hash: CryptoHash,
        epoch_height: EpochHeight,
        shards: Vec<ShardId>,
        secret_key: &SecretKey,
    ) -> Self {
        #[cfg(not(test))]
        assert_eq!(&secret_key.public_key(), peer_id.public_key());
        let hash = Self::build_hash(&sync_hash, &epoch_height, &shards);
        let signature = secret_key.sign(hash.as_ref());
        Self { peer_id, sync_hash, epoch_height, shards, signature }
    }

    pub(crate) fn hash(&self) -> CryptoHash {
        Self::build_hash(&self.sync_hash, &self.epoch_height, &self.shards)
    }

    pub(crate) fn verify(&self) -> Result<(), SnapshotHostInfoVerificationError> {
        // Number of shards must be limited, otherwise it'd be possible to create malicious
        // messages with millions of shard ids.
        if self.shards.len() > MAX_SHARDS_PER_SNAPSHOT_HOST_INFO {
            return Err(SnapshotHostInfoVerificationError::TooManyShards(self.shards.len()));
        }

        if !self.signature.verify(self.hash().as_ref(), self.peer_id.public_key()) {
            return Err(SnapshotHostInfoVerificationError::InvalidSignature);
        }

        Ok(())
    }
}

// TODO(saketh): Think about whether to add some kind of incremental sync
// vs. full sync behavior here (similar to what we have for SyncAccountsData).
// It doesn't seem necessary, but I don't fully understand why we need it for
// SyncAccountsData either so it's worth revisiting.
#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Hash,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    ProtocolSchema,
)]
pub struct SyncSnapshotHosts {
    pub hosts: Vec<Arc<SnapshotHostInfo>>,
}

#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
pub enum SnapshotHostInfoVerificationError {
    #[error("SnapshotHostInfo is signed with an invalid signature")]
    InvalidSignature,
    #[error(
        "SnapshotHostInfo contains more shards than allowed: {0} > {} (MAX_SHARDS_PER_SNAPSHOT_HOST_INFO)",
        MAX_SHARDS_PER_SNAPSHOT_HOST_INFO
    )]
    TooManyShards(usize),
}
