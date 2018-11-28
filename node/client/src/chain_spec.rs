/// Specification of the blockchain in general.
pub struct ChainSpec {
    /// Genesis state balances.
    pub balances: Vec<(String, u128)>,
    /// Genesis state authorities that bootstrap the chain.
    pub initial_authorities: Vec<String>,
}
