use primitives::types::{AccountAlias, PublicKeyAlias};

/// Specification of the blockchain in general.
pub struct ChainSpec {
    /// Genesis state accounts.
    pub accounts: Vec<(AccountAlias, PublicKeyAlias, u64)>,

    /// Genesis smart contract code.
    pub genesis_wasm: Vec<u8>,

    /// Genesis state authorities that bootstrap the chain.
    pub initial_authorities: Vec<PublicKeyAlias>,
}
