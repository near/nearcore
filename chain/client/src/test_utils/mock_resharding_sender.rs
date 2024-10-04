use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use near_async::messaging::CanSend;
use near_chain::resharding::types::FlatStorageSplitShardRequest;

#[derive(Clone, Default)]
pub struct MockReshardingSender {
    split_shard_request: Arc<RwLock<VecDeque<FlatStorageSplitShardRequest>>>,
}

impl CanSend<FlatStorageSplitShardRequest> for MockReshardingSender {
    fn send(&self, msg: FlatStorageSplitShardRequest) {
        self.split_shard_request.write().unwrap().push_back(msg);
    }
}
