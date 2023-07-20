use near_account_id::AccountId;
use near_chain_configs::Genesis;
use near_primitives::receipt::ReceiptResult;

pub fn mainnet_restored_receipts() -> ReceiptResult {
    let data = include_bytes!("../res/mainnet_restored_receipts.json");
    serde_json::from_slice(data)
        .expect("File with receipts restored after apply_chunks fix has to be correct")
}

pub fn mainnet_storage_usage_delta() -> Vec<(AccountId, u64)> {
    let data = include_bytes!("../res/storage_usage_delta.json");
    serde_json::from_slice(data).expect("File with storage usage delta has to be correct")
}

pub fn mainnet_genesis() -> Genesis {
    let data = include_bytes!("../res/mainnet_genesis.json");
    serde_json::from_slice(data).expect("Failed to deserialize mainnet genesis")
}
