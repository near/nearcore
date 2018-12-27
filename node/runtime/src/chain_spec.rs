use primitives::types::{AccountId, ReadablePublicKey};

/// Specification of the blockchain in general.
pub struct ChainSpec {
    /// Genesis state accounts.
    pub accounts: Vec<(AccountId, ReadablePublicKey, u64)>,

    /// Genesis smart contract code.
    pub genesis_wasm: Vec<u8>,

    /// Genesis state authorities that bootstrap the chain.
    pub initial_authorities: Vec<(AccountId, ReadablePublicKey, u64)>,

    pub beacon_chain_epoch_length: u64,
    pub beacon_chain_num_seats_per_slot: u64,

    pub boot_nodes: Vec<String>,
}
