use crate::signature::{KeyType, PublicKey, SecretKey};
use crate::{InMemorySigner, Signature};

fn ed25519_key_pair_from_seed(
    seed: &str,
) -> (sodiumoxide::crypto::sign::ed25519::PublicKey, sodiumoxide::crypto::sign::ed25519::SecretKey)
{
    let seed_bytes = seed.as_bytes();
    let len = seed_bytes.len();
    let mut seed: [u8; 32] = [b' '; 32];
    seed[..len].copy_from_slice(&seed_bytes[..len]);
    sodiumoxide::crypto::sign::ed25519::keypair_from_seed(
        &sodiumoxide::crypto::sign::ed25519::Seed(seed),
    )
}

impl PublicKey {
    pub fn from_seed(key_type: KeyType, seed: &str) -> Self {
        match key_type {
            KeyType::ED25519 => {
                let (public_key, _) = ed25519_key_pair_from_seed(seed);
                PublicKey::ED25519(public_key)
            }
            _ => unimplemented!(),
        }
    }
}

impl SecretKey {
    pub fn from_seed(key_type: KeyType, seed: &str) -> Self {
        match key_type {
            KeyType::ED25519 => {
                let (_, secret_key) = ed25519_key_pair_from_seed(seed);
                SecretKey::ED25519(secret_key)
            }
            _ => unimplemented!(),
        }
    }
}

const SIG: [u8; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES] =
    [0u8; sodiumoxide::crypto::sign::ed25519::SIGNATUREBYTES];

const DEFAULT_ED25519_SIGNATURE: sodiumoxide::crypto::sign::ed25519::Signature =
    sodiumoxide::crypto::sign::ed25519::Signature(SIG);

impl Signature {
    /// Empty signature that doesn't correspond to anything.
    pub fn empty(key_type: KeyType) -> Self {
        match key_type {
            KeyType::ED25519 => Signature::ED25519(DEFAULT_ED25519_SIGNATURE),
            _ => unimplemented!(),
        }
    }
}

impl InMemorySigner {
    pub fn from_random(account_id: String, key_type: KeyType) -> Self {
        let secret_key = SecretKey::from_random(key_type);
        Self { account_id, public_key: secret_key.public_key(), secret_key }
    }
}
