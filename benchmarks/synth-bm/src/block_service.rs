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

/// Read RPC URLs from a file, one URL per line
pub fn read_rpc_urls(file_path: &PathBuf) -> io::Result<Vec<String>> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    let mut urls = Vec::new();

    for line in reader.lines() {
        let url = line?;
        let trimmed = url.trim();
        if !trimmed.is_empty() && !trimmed.starts_with('#') {
            // Check if the line is just an IP address without protocol and port
            if trimmed.chars().all(|c| c.is_digit(10) || c == '.') {
                urls.push(format!("http://{}:3030", trimmed));
            } else if trimmed.contains('|') {
                // Handle complex format like "mocknet-mainnet-138038233-hoptnet-573c|us-central1-a|35.238.229.51"
                // Extract the IP address which is the last part after the last pipe
                let parts: Vec<&str> = trimmed.split('|').collect();
                if let Some(ip) = parts.last() {
                    if !ip.is_empty() {
                        urls.push(format!("http://{}:3030", ip.trim()));
                    }
                }
            } else {
                urls.push(trimmed.to_string());
            }
        }
    }

    Ok(urls)
}
