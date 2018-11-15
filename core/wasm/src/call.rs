use primitives::types::{ContractAddress};

pub struct Callback {
    contract: ContractAddress,
    version: u64,
    method: Vec<u8>,
    args: Vec<u8>,
}