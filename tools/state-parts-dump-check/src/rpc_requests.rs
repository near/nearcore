use serde::Deserialize;
use std::error::Error;

#[derive(Deserialize)]
struct ValidatorsResponse {
    result: ValidatorsInfo,
}

#[derive(Deserialize)]
struct BlockResponse {
    result: BlockInfo,
}

#[derive(Deserialize)]
struct ValidatorsInfo {
    epoch_start_height: u64,
    epoch_height: u64,
}

#[derive(Deserialize)]
struct BlockInfo {
    header: BlockInfoHeader,
    chunks: Vec<ChunkInfo>,
}

#[derive(Deserialize)]
struct BlockInfoHeader {
    hash: String,
    prev_hash: String,
    epoch_id: String,
}

#[derive(Deserialize)]
struct ChunkInfo {
    prev_state_root: String,
}

pub struct RpcClient {
    client: reqwest::blocking::Client,
    rpc_endpoint: String,
}

impl RpcClient {
    // network can only be "mainnet" or "testnet"
    pub fn new(network: &str) -> Self {
        Self {
            client: reqwest::blocking::Client::new(),
            rpc_endpoint: format!("https://rpc.{}.near.org", network),
        }
    }

    pub fn get_block_hash(&self, block_height: u64) -> Result<String, Box<dyn Error>> {
        let block_response: BlockResponse = self
            .client
            .post(&self.rpc_endpoint)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": "block",
                "params": {
                    "block_id": block_height
                }
            }))
            .send()?
            .json()?;
        let block_hash = block_response.result.header.hash;

        Ok(block_hash)
    }

    // epoch_id here is the epoch_id you want state part for
    pub fn get_current_epoch_first_block_hash(
        &self,
        epoch_id: &String,
    ) -> Result<String, Box<dyn Error>> {
        let epoch_response: ValidatorsResponse = self
            .client
            .post(&self.rpc_endpoint)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": "validators",
                "params": {
                    "epoch_id": epoch_id
                }
            }))
            .send()?
            .json()?;
        let epoch_start_height = epoch_response.result.epoch_start_height;
        let epoch_start_block_hash = self.get_block_hash(epoch_start_height).unwrap();
        Ok(epoch_start_block_hash)
    }

    pub fn get_prev_epoch_last_block_hash(
        &self,
        epoch_id: &String,
    ) -> Result<String, Box<dyn Error>> {
        let current_epoch_first_block_hash =
            self.get_current_epoch_first_block_hash(epoch_id).unwrap();
        let current_epoch_first_block_response: BlockResponse = self
            .client
            .post(&self.rpc_endpoint)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": "block",
                "params": {
                    "block_id": current_epoch_first_block_hash
                }
            }))
            .send()?
            .json()?;
        let last_epoch_last_block_hash = current_epoch_first_block_response.result.header.prev_hash;
        Ok(last_epoch_last_block_hash)
    }

    pub fn get_prev_epoch_id(&self, epoch_id: &String) -> Result<String, Box<dyn Error>> {
        let prev_epoch_last_block_hash = self.get_prev_epoch_last_block_hash(epoch_id).unwrap();
        let prev_epoch_last_block_response: BlockResponse = self
            .client
            .post(&self.rpc_endpoint)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": "block",
                "params": {
                    "block_id": prev_epoch_last_block_hash
                }
            }))
            .send()?
            .json()?;
        Ok(prev_epoch_last_block_response.result.header.epoch_id)
    }

    // get the state root of previous epoch. This should be the state root of the last block of last epoch.
    pub fn get_prev_epoch_state_root(&self, block_hash: &String, shard_id: u64) -> Result<String, Box<dyn Error>> {
        let prev_epoch_last_block_hash = self.get_prev_epoch_last_block_hash(block_hash).unwrap();
        let prev_epoch_last_block_response: BlockResponse = self
            .client
            .post(&self.rpc_endpoint)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": "block",
                "params": {
                    "block_id": prev_epoch_last_block_hash
                }
            }))
            .send()?
            .json()?;
        let chunks = prev_epoch_last_block_response.result.chunks;
        let state_root = &chunks[shard_id as usize] .prev_state_root;
        Ok(state_root.to_string())
    }

    pub fn get_final_block_hash(&self) -> Result<String, Box<dyn Error>> {
        let status_response: BlockResponse = self
            .client
            .post(&self.rpc_endpoint)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": "block",
                "params": {
                    "finality": "final"
                }
            }))
            .send()?
            .json()?;
        let latest_block_hash = status_response.result.header.hash;
        Ok(latest_block_hash)
    }

    pub fn get_epoch_id(&self, block_hash: &String) -> Result<String, Box<dyn Error>> {
        let block_response: BlockResponse = self
            .client
            .post(&self.rpc_endpoint)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": "block",
                "params": {
                    "block_id": block_hash
                }
            }))
            .send()?
            .json()?;
        let epoch_id = block_response.result.header.epoch_id;
        Ok(epoch_id)
    }

    pub fn get_epoch_height(&self, epoch_id: &String) -> Result<u64, Box<dyn Error>> {
        let block_response: ValidatorsResponse = self
            .client
            .post(&self.rpc_endpoint)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": "validators",
                "params": {
                    "epoch_id": epoch_id
                }
            }))
            .send()?
            .json()?;
        Ok(block_response.result.epoch_height)
    }
}
