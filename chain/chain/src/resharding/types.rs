use crate::flat_storage_resharder::FlatStorageResharder;
use near_async::messaging::Sender;

#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct FlatStorageSplitShardRequest {
    pub resharder: FlatStorageResharder,
}

#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
pub struct ReshardingSender {
    pub flat_storage_split_shard_send: Sender<FlatStorageSplitShardRequest>,
}
