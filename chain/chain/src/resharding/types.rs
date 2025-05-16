use crate::flat_storage_resharder::FlatStorageResharder;
use near_async::messaging::Sender;
use near_store::ShardUId;

/// Represents a request to start the split of a parent shard flat storage into two children flat
/// storages.
#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct FlatStorageSplitShardRequest {
    pub resharder: FlatStorageResharder,
}

/// Represents a request to start the catchup phase of a flat storage child shard.
#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct FlatStorageShardCatchupRequest {
    pub resharder: FlatStorageResharder,
    pub shard_uid: ShardUId,
}

/// Represents a request to reload a Mem Trie for a shard after its Flat Storage resharding is
/// finished.
#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct MemtrieReloadRequest {
    pub shard_uid: ShardUId,
}

/// A multi-sender for the FlatStorageResharder post processing API.
///
/// This is meant to be used to send messages to handle the post processing tasks needed for
/// resharding the flat storage. An example is splitting a shard.
#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
pub struct ReshardingSender {
    pub flat_storage_split_shard_sender: Sender<FlatStorageSplitShardRequest>,
    pub flat_storage_shard_catchup_sender: Sender<FlatStorageShardCatchupRequest>,
    pub memtrie_reload_sender: Sender<MemtrieReloadRequest>,
}
