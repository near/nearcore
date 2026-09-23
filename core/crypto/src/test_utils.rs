use crate::signature::{
    KeyType, ML_DSA_65_SIGNATURE_LENGTH, MlDsa65Signature, PublicKey, SecretKey,
};
#[cfg(feature = "rand")]
use crate::signature::{
    ML_DSA_65_PUBLIC_KEY_LENGTH, ML_DSA_65_SEED_LENGTH, MlDsa65PublicKey, ml_dsa_65_from_seed,
};
use crate::{InMemorySigner, Signature};

#[cfg(feature = "rand")]
fn ed25519_key_pair_from_seed(seed: &str) -> ed25519_dalek::SigningKey {
    let seed_bytes = seed.as_bytes();
    let len = std::cmp::min(ed25519_dalek::SECRET_KEY_LENGTH, seed_bytes.len());
    let mut seed: [u8; ed25519_dalek::SECRET_KEY_LENGTH] = [b' '; ed25519_dalek::SECRET_KEY_LENGTH];
    seed[..len].copy_from_slice(&seed_bytes[..len]);
    ed25519_dalek::SigningKey::from_bytes(&seed)
}

#[cfg(feature = "rand")]
fn secp256k1_secret_key_from_seed(seed: &str) -> secp256k1::SecretKey {
    use secp256k1::rand::SeedableRng;

    let seed_bytes = seed.as_bytes();
    let len = std::cmp::min(32, seed_bytes.len());
    let mut seed: [u8; 32] = [b' '; 32];
    seed[..len].copy_from_slice(&seed_bytes[..len]);
    let mut rng = secp256k1::rand::rngs::StdRng::from_seed(seed);
    secp256k1::SecretKey::new(&mut rng)
}

/// Derive a deterministic 32-byte seed from a free-form `&str` for
/// ML-DSA-65 test key derivation. Mirrors the ed25519/secp256k1 zero-pad
/// pattern used elsewhere in this file: take the first 32 bytes of the
/// input and pad with spaces. Long inputs collide on the same seed; that
/// is acceptable for test fixtures.
#[cfg(feature = "rand")]
fn ml_dsa_65_seed_bytes_from_str(seed: &str) -> [u8; ML_DSA_65_SEED_LENGTH] {
    let seed_bytes = seed.as_bytes();
    let len = std::cmp::min(ML_DSA_65_SEED_LENGTH, seed_bytes.len());
    let mut out: [u8; ML_DSA_65_SEED_LENGTH] = [b' '; ML_DSA_65_SEED_LENGTH];
    out[..len].copy_from_slice(&seed_bytes[..len]);
    out
}

impl PublicKey {
    #[cfg(feature = "rand")]
    pub fn from_seed(key_type: KeyType, seed: &str) -> Self {
        match key_type {
            KeyType::ED25519 => {
                let keypair = ed25519_key_pair_from_seed(seed);
                PublicKey::ED25519(crate::signature::ED25519PublicKey(
                    keypair.verifying_key().to_bytes(),
                ))
            }
            KeyType::SECP256K1 => {
                let secret_key = SecretKey::SECP256K1(secp256k1_secret_key_from_seed(seed));
                PublicKey::SECP256K1(secret_key.public_key().unwrap_as_secp256k1().clone())
            }
            KeyType::MLDSA65 => {
                let seed_bytes = ml_dsa_65_seed_bytes_from_str(seed);
                let sk = ml_dsa_65_from_seed(&seed_bytes).expect("ML-DSA-65 from_seed failed");
                sk.public_key()
            }
        }
    }
}

impl SecretKey {
    #[cfg(feature = "rand")]
    pub fn from_seed(key_type: KeyType, seed: &str) -> Self {
        match key_type {
            KeyType::ED25519 => {
                let keypair = ed25519_key_pair_from_seed(seed);
                SecretKey::ED25519(crate::signature::ED25519SecretKey(keypair.to_keypair_bytes()))
            }
            KeyType::SECP256K1 => SecretKey::SECP256K1(secp256k1_secret_key_from_seed(seed)),
            KeyType::MLDSA65 => {
                let seed_bytes = ml_dsa_65_seed_bytes_from_str(seed);
                ml_dsa_65_from_seed(&seed_bytes).expect("ML-DSA-65 from_seed failed")
            }
        }
    }
}

const SIG: [u8; ed25519_dalek::SIGNATURE_LENGTH] = [0u8; ed25519_dalek::SIGNATURE_LENGTH];

impl Signature {
    /// Empty signature that doesn't correspond to anything.
    pub fn empty(key_type: KeyType) -> Self {
        match key_type {
            KeyType::ED25519 => Signature::ED25519(ed25519_dalek::Signature::from_bytes(&SIG)),
            KeyType::MLDSA65 => {
                Signature::MLDSA65(MlDsa65Signature(Box::new([0u8; ML_DSA_65_SIGNATURE_LENGTH])))
            }
            _ => unimplemented!(),
        }
    }
}

#[cfg(feature = "rand")]
impl PublicKey {
    /// Construct an all-zero ML-DSA-65 pubkey for tests where the key data
    /// doesn't matter (e.g. verifying that infrastructure plumbing handles a
    /// large-payload variant). NOT a valid pubkey for verification.
    pub fn ml_dsa_65_zero() -> Self {
        PublicKey::MLDSA65(MlDsa65PublicKey(Box::new([0u8; ML_DSA_65_PUBLIC_KEY_LENGTH])))
    }
}

impl InMemorySigner {
    #[cfg(feature = "rand")]
    pub fn from_random(account_id: near_account_id::AccountId, key_type: KeyType) -> Self {
        let secret_key = SecretKey::from_random(key_type);
        Self { account_id, public_key: secret_key.public_key(), secret_key }
    }
}
