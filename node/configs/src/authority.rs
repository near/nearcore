use primitives::crypto::aggregate_signature::BlsPublicKey;
use primitives::crypto::signature::PublicKey;
use primitives::traits::Base58Encoded;
use primitives::types::AuthorityStake;
use std::convert::TryFrom;

use crate::chain_spec::{AuthorityRotation, ChainSpec};

/// Configure the authority rotation.
#[derive(Clone)]
pub struct AuthorityConfig {
    /// List of initial proposals at genesis block.
    pub initial_proposals: Vec<AuthorityStake>,
    /// Authority rotation.
    pub authority_rotation: AuthorityRotation,
}

pub fn get_authority_config(chain_spec: &ChainSpec) -> AuthorityConfig {
    let initial_authorities: Vec<AuthorityStake> = chain_spec
        .initial_authorities
        .iter()
        .map(|(account_id, public_key, bls_public_key, amount)| AuthorityStake {
            account_id: account_id.clone(),
            public_key: PublicKey::try_from(public_key.0.as_str()).unwrap(),
            bls_public_key: BlsPublicKey::from_base58(&bls_public_key.0).unwrap(),
            amount: *amount,
        })
        .collect();
    AuthorityConfig {
        initial_proposals: initial_authorities,
        authority_rotation: chain_spec.authority_rotation.clone(),
    }
}
