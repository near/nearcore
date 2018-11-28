use primitives::types::AccountAlias;

/// Specification of the blockchain in general.
pub struct ChainSpec {
    /// Genesis state balances.
    pub balances: Vec<(AccountAlias, u64)>,
    /// Genesis state authorities that bootstrap the chain.
    pub initial_authorities: Vec<AccountAlias>,
    pub genesis_wasm: Vec<u8>,
}
