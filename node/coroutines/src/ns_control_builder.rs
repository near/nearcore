//! Constructs control for Nightshade using the current Client state.
use rand::{SeedableRng, XorShiftRng};

use client::Client;
use mempool::pool_task::MemPoolControl;
use primitives::aggregate_signature::BlsSecretKey;
use primitives::test_utils::get_key_pair_from_seed;
use primitives::types::AuthorityId;

pub fn get_control(client: &Client, block_index: u64) -> MemPoolControl {
    // TODO: Get authorities for the correct block index. For now these are the same authorities
    // that built the first block. In other words use `block_index` instead of `mock_block_index`.
    let mock_block_index = 2;
    let (owner_uid, uid_to_authority_map) = client.get_uid_to_authority_map(mock_block_index);
    if owner_uid.is_none() {
        return MemPoolControl::Stop;
    }
    let owner_uid = owner_uid.unwrap();
    let num_authorities = uid_to_authority_map.len();

    // TODO: This is a temporary hack that generates public and secret keys
    // for all participants inside each participant.
    let mut public_keys = vec![];
    let mut secret_keys = vec![];
    let mut bls_public_keys = vec![];
    let mut bls_secret_keys = vec![];
    for i in 0..num_authorities {
        let mut seed: [u8; 32] = [b' '; 32];
        seed[0] = i as u8;
        let (public_key, secret_key) = get_key_pair_from_seed(&String::from_utf8_lossy(&seed));
        public_keys.push(public_key);
        secret_keys.push(secret_key);
    }
    for i in 0..num_authorities {
        let mut rng = XorShiftRng::from_seed([i as u32 + 1, 0, 0, 0]);
        let bls_secret_key = BlsSecretKey::generate_from_rng(&mut rng);
        let bls_public_key = bls_secret_key.get_public_key();
        bls_public_keys.push(bls_public_key);
        bls_secret_keys.push(bls_secret_key);
    }
    let owner_secret_key = secret_keys[owner_uid as AuthorityId].clone();
    let bls_owner_secret_key = bls_secret_keys[owner_uid as AuthorityId].clone();

    MemPoolControl::Reset {
        authority_id: owner_uid as AuthorityId,
        num_authorities,
        owner_uid,
        block_index,
        public_keys,
        owner_secret_key,
        bls_public_keys,
        bls_owner_secret_key,
    }
}
