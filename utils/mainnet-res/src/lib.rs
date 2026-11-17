use near_chain_configs::Genesis;

pub fn mainnet_genesis() -> Genesis {
    let data = include_bytes!("../res/mainnet_genesis.json");
    serde_json::from_slice(data).expect("Failed to deserialize mainnet genesis")
}
