use primitives::types::AccountId;

pub struct ChainSpec {
    pub balances: Vec<(AccountId, u64)>,
    pub initial_authorities: Vec<AccountId>,
    pub genesis_wasm: Vec<u8>,
}
