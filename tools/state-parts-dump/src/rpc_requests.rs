use serde::Deserialize;
use std::error::Error;

#[derive(Deserialize)]
struct ValidatorsResponse {
    result: EpochInfo,
}

#[derive(Deserialize)]
struct BlockResponse {
    result: BlockInfo,
}

#[derive(Deserialize)]
struct EpochInfo {
    epoch_start_height: u64,
}

#[derive(Deserialize)]
struct BlockInfo {
    header: BlockInfoHeader,
}

#[derive(Deserialize)]
struct BlockInfoHeader {
    hash: String,
}

#[derive(Deserialize)]
struct StatusResponse {
    result: NodeStatus,
}

#[derive(Deserialize)]
struct NodeStatus {
    sync_info: SyncInfo,
}

#[derive(Deserialize)]
struct SyncInfo {
    genesis_hash: String,
    head_height: u64,
}

pub struct RpcClient {
    client: reqwest::blocking::Client,
    rpc_endpoint: &str,
}

impl RpcClient {
    // network can only be "mainnet" or "testnet"
    pub fn new(network: &str) -> Self {
        let rpc_end_point = format!("https://rpc.{}.near.org", network);
        Self { client: reqwest::blocking::Client::new(), rpc_endpoint: rpc_endpoint }
    }

    pub fn get_epoch_start_block_hash(&self, epoch_id: &String) -> Result<String, Box<dyn Error>> {
        let epoch_id = epoch_id.clone();

        // Step 1: Get epoch_start_height
        let validators_response: ValidatorResponse = self
            .client
            .post(RPC_ENDPOINT)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": "validators",
                "params": {
                    epoch_id: epoch_id
                }
            }))
            .send()?
            .json()?;
        let epoch_start_height = validators_response.result.epoch_start_height;
        println!("Epoch Start Height: {}", epoch_start_height);

        // Step 2: Get block hash
        let block_response: BlockResponse = self
            .client
            .post(RPC_ENDPOINT)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": "block",
                "params": {
                    block_id: epoch_start_height
                }
            }))
            .send()?
            .json()?;
        let epoch_start_block_hash = block_response.result.header.hash;
        println!("Epoch start block hash: {}", epoch_start_block_hash);

        Ok(epoch_start_block_hash)
    }

    pub fn get_genesis_hash(&self) -> Result<String, Box<dyn Error>> {
        let response: StatusResponse = self
            .client
            .post(RPC_ENDPOINT)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": "status",
                "params": {}
            }))
            .send()?
            .json()?;

        let genesis_hash = response.result.sync_info.genesis_hash;
        println!("Genesis Hash: {}", genesis_hash);

        Ok(genesis_hash)
    }

    pub fn get_head_height(&self) -> Result<u64, Box<dyn Error>> {
        let response: StatusResponse = self
            .client
            .post(RPC_ENDPOINT)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": "status",
                "params": {}
            }))
            .send()?
            .json()?;

        let head_height = response.result.sync_info.head_height;
        println!("Head height: {}", head_height);

        Ok(head_height)
    }
}
