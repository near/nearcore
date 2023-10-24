use near_store::ShardUId;

/// State sync response from peers.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub enum StateSyncResponse {
    HeaderResponse,
    PartResponse,
}

/// A strongly typed asynchronous API for the State Sync logic
/// It abstracts away the fact that it is implemented using actix
/// actors.
#[async_trait::async_trait]
pub trait StateSync: Send + Sync + 'static {
    async fn send(&mut self, shard_uid: ShardUId, msg: StateSyncResponse);
}
