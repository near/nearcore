use exonum_sodiumoxide::crypto::sign::ed25519::{keypair_from_seed, Seed};
use rand::{SeedableRng, XorShiftRng};

use crate::aggregate_signature::{BlsPublicKey, BlsSecretKey};
use crate::signature::{PublicKey, SecretKey};
use crate::signer::InMemorySigner;

pub fn get_key_pair_from_seed(seed_string: &str) -> (PublicKey, SecretKey) {
    let mut seed: [u8; 32] = [b' '; 32];
    let len = ::std::cmp::min(32, seed_string.len());
    seed[..len].copy_from_slice(&seed_string.as_bytes()[..len]);

    let (public_key, secret_key) = keypair_from_seed(&Seed(seed));
    (PublicKey(public_key), SecretKey(secret_key))
}

pub fn get_bls_key_pair_from_seed(seed_string: &str) -> (BlsPublicKey, BlsSecretKey) {
    let mut seed: [u8; 16] = [b' '; 16];
    let len = ::std::cmp::min(4, seed_string.len());
    seed[..len].copy_from_slice(&seed_string.as_bytes()[..len]);

    let mut seed32: [u32; 4] = [0; 4];
    for i in 0..4 {
        seed32[i] = u32::from(seed[i * 4])
            + (u32::from(seed[i * 4 + 1]) << 8)
            + (u32::from(seed[i * 4 + 2]) << 16)
            + (u32::from(seed[i * 4 + 3]) << 24);
    }
    let mut rng = XorShiftRng::from_seed(seed32);
    let bls_secret_key = BlsSecretKey::generate_from_rng(&mut rng);
    (bls_secret_key.get_public_key(), bls_secret_key)
}

impl InMemorySigner {
    pub fn from_seed(account_id: &str, seed_string: &str) -> Self {
        let (public_key, secret_key) = get_key_pair_from_seed(seed_string);
        let (bls_public_key, bls_secret_key) = get_bls_key_pair_from_seed(seed_string);
        InMemorySigner {
            account_id: account_id.to_string(),
            public_key,
            secret_key,
            bls_public_key,
            bls_secret_key,
        }
    }
}
