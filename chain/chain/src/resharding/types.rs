use crate::flat_storage_resharder::FlatStorageResharder;
use near_async::messaging::Sender;
use near_primitives::hash::CryptoHash;
use near_store::ShardUId;

/// Represents a request to start various stages of resharding.
#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "()")]
pub enum ReshardingRequest {
    /// Split a parent shard flat storage into two children flat storages.
    FlatStorageSplitShard { resharder: FlatStorageResharder },
    /// Perform catchup on a flat storage shard.
    FlatStorageShardCatchup {
        resharder: FlatStorageResharder,
        shard_uid: ShardUId,
        flat_head_block_hash: CryptoHash,
    },
    /// Memtrie for a shard is ready to be rebuilt from flat storage.
    MemtrieReload { shard_uid: ShardUId },
}

/// A multi-sender for the FlatStorageResharder post processing API.
///
/// This is meant to be used to send messages to handle the post processing tasks needed for
/// resharding V3. An example is splitting a shard flat storage.
#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
pub struct ReshardingSender {
    pub sender: Sender<ReshardingRequest>,
}
