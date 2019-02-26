//! Constructs control for Nightshade using the current Client state.
use client::Client;
use exonum_sodiumoxide::crypto::sign::ed25519::{keypair_from_seed, Seed};
use nightshade::nightshade_task::Control;
use primitives::aggregate_signature::BlsSecretKey;
use primitives::chain::ChainPayload;
use primitives::signature::PublicKey;
use primitives::signature::SecretKey;
use rand::{SeedableRng, XorShiftRng};

pub fn get_control(client: &Client, block_index: u64) -> Control<ChainPayload> {
    let (owner_uid, uid_to_authority_map) = client.get_uid_to_authority_map(block_index);
    if owner_uid.is_none() {
        return Control::Stop;
    }
    let owner_uid = owner_uid.unwrap();
    let num_authorities = uid_to_authority_map.len();
    // TODO: Each participant should propose different chain payloads.
    let payload: ChainPayload = ChainPayload { transactions: vec![], receipts: vec![] };

    // TODO: This is a temporary hack that generates public and secret keys
    // for all participants inside each participant.
    let mut public_keys = vec![];
    let mut secret_keys = vec![];
    let mut bls_public_keys = vec![];
    let mut bls_secret_keys = vec![];
    for i in 0..num_authorities {
        let mut seed: [u8; 32] = [b' '; 32];
        seed[0] = i as u8;
        let (public_key, secret_key) = keypair_from_seed(&Seed(seed));
        public_keys.push(PublicKey(public_key));
        secret_keys.push(SecretKey(secret_key));
    }
    for i in 0..num_authorities {
        let mut rng = XorShiftRng::from_seed([i as u32, 0, 0, 0]);
        let bls_secret_key = BlsSecretKey::generate_from_rng(&mut rng);
        let bls_public_key = bls_secret_key.get_public_key();
        bls_public_keys.push(bls_public_key);
        bls_secret_keys.push(bls_secret_key);
    }
    let owner_secret_key = secret_keys[owner_uid as usize].clone();
    let bls_owner_secret_key = bls_secret_keys[owner_uid as usize].clone();

    Control::Reset {
        owner_uid,
        block_index,
        payload,
        public_keys,
        owner_secret_key,
        bls_public_keys,
        bls_owner_secret_key,
    }
}
