use near_crypto::SecretKey;
use near_crypto::Signature;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::ShardId;

// TODO(saketh): Consider moving other types related to state sync into this file
// e.g. StateResponseInfo

/// Specifies information about a state snapshot hosted by a network node.
///
/// A signature is included so that we know it was really published by the indicated peer.
///
#[derive(Clone, Debug, Eq, PartialEq, borsh::BorshSerialize, borsh::BorshDeserialize)]
pub struct SnapshotHostInfo {
    /// Id of the node serving the snapshot
    pub peer_id: PeerId,
    /// Hash of the snapshot's state root
    pub sync_hash: CryptoHash,
    /// List of shards included in the snapshot
    pub shards: Vec<ShardId>,
    /// Signature on (sync_hash, shards)
    pub signature: Signature,
}

impl SnapshotHostInfo {
    // Hash of `sync_hash` and `shards`
    fn build_hash(sync_hash: &CryptoHash, shards: &Vec<ShardId>) -> CryptoHash {
        CryptoHash::hash_borsh((sync_hash, shards))
    }

    pub(crate) fn new(
        peer_id: PeerId,
        sync_hash: CryptoHash,
        shards: Vec<ShardId>,
        secret_key: &SecretKey,
    ) -> Self {
        assert_eq!(&secret_key.public_key(), peer_id.public_key());
        let hash = Self::build_hash(&sync_hash, &shards);
        let signature = secret_key.sign(hash.as_ref());
        Self { peer_id, sync_hash, shards, signature }
    }

    pub(crate) fn hash(&self) -> CryptoHash {
        Self::build_hash(&self.sync_hash, &self.shards)
    }

    pub(crate) fn verify(&self) -> bool {
        self.signature.verify(self.hash().as_ref(), self.peer_id.public_key())
    }
}

// TODO(saketh): Think about whether to add some kind of incremental sync
// vs. full sync behavior here (similar to what we have for SyncAccountsData).
// It doesn't seem necessary, but I don't fully understand why we need it for
// SyncAccountsData either so it's worth revisiting.
#[derive(Clone, Debug, Eq, PartialEq, borsh::BorshSerialize, borsh::BorshDeserialize)]
pub struct SyncSnapshotHosts {
    pub hosts: Vec<SnapshotHostInfo>,
}
