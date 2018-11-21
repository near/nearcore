use primitives::types::AccountId;

pub struct ChainSpec {
    pub balances: Vec<(AccountId, u128)>,
    pub initial_authorities: Vec<AccountId>,
}
