use primitives::types::AuthorityStake;

use crate::chain_spec::ChainSpec;

/// Configure the authority rotation.
pub struct AuthorityConfig {
    /// List of initial proposals at genesis block.
    pub initial_proposals: Vec<AuthorityStake>,
    /// Authority epoch length.
    pub epoch_length: u64,
    /// Number of seats per slot.
    pub num_seats_per_slot: u64,
}

pub fn get_authority_config(chain_spec: &ChainSpec) -> AuthorityConfig {
    let initial_authorities: Vec<AuthorityStake> = chain_spec
        .initial_authorities
        .iter()
        .map(|(account_id, key, amount)| AuthorityStake {
            account_id: account_id.clone(),
            public_key: key.into(),
            amount: *amount,
        })
        .collect();
    AuthorityConfig {
        initial_proposals: initial_authorities,
        epoch_length: chain_spec.beacon_chain_epoch_length,
        num_seats_per_slot: chain_spec.beacon_chain_num_seats_per_slot,
    }
}
