use secp256k1::rand::SeedableRng;

use crate::signature::{ED25519PublicKey, ED25519SecretKey, KeyType, PublicKey, SecretKey};
use crate::{InMemorySigner, Signature};
use near_account_id::AccountId;

fn ed25519_key_pair_from_seed(seed: &str) -> ed25519_dalek::Keypair {
    let seed_bytes = seed.as_bytes();
    let len = std::cmp::min(ed25519_dalek::SECRET_KEY_LENGTH, seed_bytes.len());
    let mut seed: [u8; ed25519_dalek::SECRET_KEY_LENGTH] = [b' '; ed25519_dalek::SECRET_KEY_LENGTH];
    seed[..len].copy_from_slice(&seed_bytes[..len]);
    let secret = ed25519_dalek::SecretKey::from_bytes(&seed).unwrap();
    let public = ed25519_dalek::PublicKey::from(&secret);
    ed25519_dalek::Keypair { secret, public }
}

fn secp256k1_secret_key_from_seed(seed: &str) -> secp256k1::SecretKey {
    let seed_bytes = seed.as_bytes();
    let len = std::cmp::min(32, seed_bytes.len());
    let mut seed: [u8; 32] = [b' '; 32];
    seed[..len].copy_from_slice(&seed_bytes[..len]);
    let mut rng = secp256k1::rand::rngs::StdRng::from_seed(seed);
    secp256k1::SecretKey::new(&mut rng)
}

impl PublicKey {
    pub fn from_seed(key_type: KeyType, seed: &str) -> Self {
        match key_type {
            KeyType::ED25519 => {
                let keypair = ed25519_key_pair_from_seed(seed);
                PublicKey::ED25519(ED25519PublicKey(keypair.public.to_bytes()))
            }
            _ => unimplemented!(),
        }
    }
}

impl SecretKey {
    pub fn from_seed(key_type: KeyType, seed: &str) -> Self {
        match key_type {
            KeyType::ED25519 => {
                let keypair = ed25519_key_pair_from_seed(seed);
                SecretKey::ED25519(ED25519SecretKey(keypair.to_bytes()))
            }
            _ => SecretKey::SECP256K1(secp256k1_secret_key_from_seed(seed)),
        }
    }
}

const SIG: [u8; ed25519_dalek::SIGNATURE_LENGTH] = [0u8; ed25519_dalek::SIGNATURE_LENGTH];

impl Signature {
    /// Empty signature that doesn't correspond to anything.
    pub fn empty(key_type: KeyType) -> Self {
        match key_type {
            KeyType::ED25519 => {
                Signature::ED25519(ed25519_dalek::Signature::from_bytes(&SIG).unwrap())
            }
            _ => unimplemented!(),
        }
    }
}

impl InMemorySigner {
    pub fn from_random(account_id: AccountId, key_type: KeyType) -> Self {
        let secret_key = SecretKey::from_random(key_type);
        Self { account_id, public_key: secret_key.public_key(), secret_key }
    }
}
