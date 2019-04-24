use crate::node::{Node, ProcessNode, ThreadNode, TEST_BLOCK_MAX_SIZE};
use crate::user::{ShardClientUser, User};
use configs::ClientConfig;
use primitives::crypto::signer::InMemorySigner;
use primitives::types::AccountId;
use shard::ShardClient;
use std::sync::Arc;
use storage::test_utils::create_beacon_shard_storages;

pub struct ShardClientNode {
    pub client: Arc<ShardClient>,
    pub signer: Arc<InMemorySigner>,
}

impl ShardClientNode {
    pub fn new(config: ClientConfig) -> Self {
        let account_id = &config.account_id.unwrap();
        let signer = Arc::new(InMemorySigner::from_seed(account_id, account_id));
        let (_, shard_storage) = create_beacon_shard_storages();

        let chain_spec = &config.chain_spec;
        let shard_client =
            ShardClient::new(Some(signer.clone()), chain_spec, shard_storage, TEST_BLOCK_MAX_SIZE);
        ShardClientNode { client: Arc::new(shard_client), signer }
    }
}

impl Node for ShardClientNode {
    fn account_id(&self) -> Option<&AccountId> {
        Some(&self.signer.account_id)
    }

    fn start(&mut self) {}

    fn kill(&mut self) {}

    fn signer(&self) -> Arc<InMemorySigner> {
        self.signer.clone()
    }

    fn as_process_mut(&mut self) -> &mut ProcessNode {
        unimplemented!()
    }

    fn as_thread_mut(&mut self) -> &mut ThreadNode {
        unimplemented!()
    }

    fn is_running(&self) -> bool {
        true
    }

    fn user(&self) -> Box<dyn User> {
        Box::new(ShardClientUser::new(self.client.clone()))
    }
}
