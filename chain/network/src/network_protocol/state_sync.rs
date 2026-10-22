use super::MAX_SHARDS_PER_SNAPSHOT_HOST_INFO;
use crate::network_protocol::Arc;
use near_crypto::SecretKey;
use near_crypto::Signature;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::state_part::StatePartIndex;
use near_primitives::types::EpochHeight;
use near_primitives::types::ShardId;
use near_schema_checker_lib::ProtocolSchema;
use std::net::SocketAddr;
// TODO(saketh): Consider moving other types related to state sync into this file
// e.g. StateResponseInfo

/// Specifies information about a state snapshot hosted by a network peer.
///
/// Signatures are included so that we know it was really published by that peer.
///
/// Discriminants are part of the borsh encoding, don't reuse them.
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
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum SnapshotHostInfo {
    /// Published by an older host, or relayed by an older node, which drops the fields of V2
    /// that it doesn't know.
    V1(SnapshotHostInfoV1) = 0,
    V2(SnapshotHostInfoV2) = 1,
}

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
pub struct SnapshotHostInfoV1 {
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
pub struct SnapshotHostInfoV2 {
    /// Id of the node serving the snapshot
    pub peer_id: PeerId,
    /// Hash of the snapshot's state root
    pub sync_hash: CryptoHash,
    /// Ordinal of the epoch of the state root
    pub epoch_height: EpochHeight,
    /// List of shards included in the snapshot.
    pub shards: Vec<ShardId>,
    /// Listening address the host advertises for itself.
    pub addr: SocketAddr,
    /// Signature on ("SnapshotHostInfoV2", sync_hash, epoch_height, shards, addr).
    pub signature: Signature,
    /// Signature on (sync_hash, epoch_height, shards), the only one older nodes verify.
    // TODO(network-overhaul): Stop sending it in the release after next. Nodes on this release
    // drop infos without it, so the next release must still send it.
    pub legacy_signature: Option<Signature>,
}

impl SnapshotHostInfoV1 {
    fn build_hash(
        sync_hash: &CryptoHash,
        epoch_height: &EpochHeight,
        shards: &Vec<ShardId>,
    ) -> CryptoHash {
        CryptoHash::hash_borsh((sync_hash, epoch_height, shards))
    }

    fn has_valid_signature(&self) -> bool {
        let hash = Self::build_hash(&self.sync_hash, &self.epoch_height, &self.shards);
        self.signature.verify(hash.as_ref(), self.peer_id.public_key())
    }
}

impl SnapshotHostInfoV2 {
    fn build_hash(
        sync_hash: &CryptoHash,
        epoch_height: &EpochHeight,
        shards: &Vec<ShardId>,
        addr: &SocketAddr,
    ) -> CryptoHash {
        // Keeps the signed data distinct from that of other versions, and from other data signed
        // with the node key, such as edges.
        static SIGNATURE_DIFFERENTIATOR: &str = "SnapshotHostInfoV2";
        CryptoHash::hash_borsh((SIGNATURE_DIFFERENTIATOR, sync_hash, epoch_height, shards, addr))
    }

    fn has_valid_signatures(&self) -> bool {
        let public_key = self.peer_id.public_key();
        let hash = Self::build_hash(&self.sync_hash, &self.epoch_height, &self.shards, &self.addr);
        self.signature.verify(hash.as_ref(), public_key)
            && self.legacy_signature.as_ref().is_none_or(|signature| {
                let legacy_hash = SnapshotHostInfoV1::build_hash(
                    &self.sync_hash,
                    &self.epoch_height,
                    &self.shards,
                );
                signature.verify(legacy_hash.as_ref(), public_key)
            })
    }
}

impl SnapshotHostInfo {
    pub(crate) fn new(
        peer_id: PeerId,
        sync_hash: CryptoHash,
        epoch_height: EpochHeight,
        shards: Vec<ShardId>,
        addr: SocketAddr,
        secret_key: &SecretKey,
    ) -> Self {
        #[cfg(not(test))]
        assert_eq!(&secret_key.public_key(), peer_id.public_key());
        let hash = SnapshotHostInfoV2::build_hash(&sync_hash, &epoch_height, &shards, &addr);
        let legacy_hash = SnapshotHostInfoV1::build_hash(&sync_hash, &epoch_height, &shards);
        Self::V2(SnapshotHostInfoV2 {
            peer_id,
            sync_hash,
            epoch_height,
            shards,
            addr,
            signature: secret_key.sign(hash.as_ref()),
            legacy_signature: Some(secret_key.sign(legacy_hash.as_ref())),
        })
    }

    pub fn peer_id(&self) -> &PeerId {
        match self {
            Self::V1(info) => &info.peer_id,
            Self::V2(info) => &info.peer_id,
        }
    }

    pub fn sync_hash(&self) -> CryptoHash {
        match self {
            Self::V1(info) => info.sync_hash,
            Self::V2(info) => info.sync_hash,
        }
    }

    pub fn epoch_height(&self) -> EpochHeight {
        match self {
            Self::V1(info) => info.epoch_height,
            Self::V2(info) => info.epoch_height,
        }
    }

    pub fn shards(&self) -> &[ShardId] {
        match self {
            Self::V1(info) => &info.shards,
            Self::V2(info) => &info.shards,
        }
    }

    /// Signature on (sync_hash, epoch_height, shards), the only one older nodes verify.
    pub(crate) fn legacy_signature(&self) -> Option<&Signature> {
        match self {
            Self::V1(info) => Some(&info.signature),
            Self::V2(info) => info.legacy_signature.as_ref(),
        }
    }

    /// Of two infos from the same host, only the one with the greater key is kept.
    /// A host publishes one info per snapshot, so the epoch height orders them. A V1 copy,
    /// such as one relayed by an older node, never replaces a V2 copy of the same epoch.
    pub(crate) fn freshness(&self) -> (EpochHeight, u8) {
        let version = match self {
            Self::V1(_) => 1,
            Self::V2(_) => 2,
        };
        (self.epoch_height(), version)
    }

    pub(crate) fn verify(&self) -> Result<(), SnapshotHostInfoVerificationError> {
        // Number of shards must be limited, otherwise it'd be possible to create malicious
        // messages with millions of shard ids.
        let num_shards = self.shards().len();
        if num_shards > MAX_SHARDS_PER_SNAPSHOT_HOST_INFO {
            return Err(SnapshotHostInfoVerificationError::TooManyShards(num_shards));
        }

        let valid = match self {
            Self::V1(info) => info.has_valid_signature(),
            Self::V2(info) => info.has_valid_signatures(),
        };
        if !valid {
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
        "SnapshotHostInfo contains more shards than allowed: \
         {0} > {MAX_SHARDS_PER_SNAPSHOT_HOST_INFO} (MAX_SHARDS_PER_SNAPSHOT_HOST_INFO)"
    )]
    TooManyShards(usize),
}

/// Message used to request a state header.
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
pub struct StateHeaderRequest {
    /// Requested shard id
    pub shard_id: ShardId,
    /// Sync block hash
    pub sync_hash: CryptoHash,
    /// Public address of the node making the request
    pub addr: std::net::SocketAddr,
}

/// Message used to request a state part.
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
pub struct StatePartRequest {
    /// Requested shard id
    pub shard_id: ShardId,
    /// Sync block hash
    pub sync_hash: CryptoHash,
    /// Requested part idx
    pub part_idx: StatePartIndex,
    /// Public address of the node making the request
    pub addr: std::net::SocketAddr,
}
