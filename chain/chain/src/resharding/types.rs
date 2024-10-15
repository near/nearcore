use crate::flat_storage_resharder::FlatStorageResharder;
use near_async::messaging::Sender;

/// Represents a request to start the split of a parent shard flat storage into two children flat
/// storages.
#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct FlatStorageSplitShardRequest {
    pub resharder: FlatStorageResharder,
}

/// A multi-sender for the FlatStorageResharder post processing API.
///
/// This is meant to be used to send messages to handle the post processing tasks needed for
/// resharding the flat storage. An example is splitting a shard.
#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
pub struct ReshardingSender {
    pub flat_storage_split_shard_send: Sender<FlatStorageSplitShardRequest>,
}
