use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use near_jsonrpc_client::JsonRpcClient;
use near_primitives::{hash::CryptoHash, views::BlockView};
use tokio::time;

use crate::rpc::get_latest_block;

pub struct BlockService {
    rpc_client: JsonRpcClient,
    refresh_interval: Duration,
    /// A block that's refreshed every `refresh_interval`.
    block: RwLock<BlockView>,
}

impl BlockService {
    /// Construction must be followed by call to `Self::start`.
    ///
    /// # Panics
    ///
    /// Panics if getting a new block fails.
    pub async fn new(rpc_client: JsonRpcClient) -> Self {
        // Getting a new block hash is relatively cheap, hence just do it every 30 seconds even
        // if longer refresh intervals might be fine too. A shorter interval reduces the chances
        // expiring transactions.
        let refresh_interval = Duration::from_secs(30);
        let block = get_latest_block(&rpc_client).await.expect("should be able to get a block");
        Self { rpc_client, refresh_interval, block: RwLock::new(block) }
    }

    /// # Panics
    ///
    /// Panics if getting a new block fails.
    pub async fn start(self: Arc<Self>) {
        let mut interval = time::interval(self.refresh_interval);

        tokio::spawn(async move {
            // First tick returns immediately, but block was just fetched in `Self::new`,
            // hence skip one tick.
            interval.tick().await;
            loop {
                interval.tick().await;
                let new_block = get_latest_block(&self.rpc_client)
                    .await
                    .expect("should be able to get a block");
                let mut block = self.block.write().unwrap();
                *block = new_block;
            }
        });
    }

    pub fn get_block(&self) -> BlockView {
        self.block.read().unwrap().clone()
    }

    pub fn get_block_hash(&self) -> CryptoHash {
        self.get_block().header.hash
    }
}
