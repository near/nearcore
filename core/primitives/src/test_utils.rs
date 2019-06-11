use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use exonum_sodiumoxide::crypto::sign::ed25519::{keypair_from_seed, Seed};
use log::LevelFilter;
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;

use crate::crypto::aggregate_signature::{BlsPublicKey, BlsSecretKey};
use crate::crypto::signature::{PublicKey, SecretKey};
use crate::crypto::signer::{EDSigner, InMemorySigner};
use crate::transaction::{SignedTransaction, TransactionBody};

pub fn init_test_logger() {
    let _ = env_logger::Builder::new()
        .filter_module("tokio_reactor", LevelFilter::Info)
        .filter_module("tokio_core", LevelFilter::Info)
        .filter_module("hyper", LevelFilter::Info)
        .filter(None, LevelFilter::Debug)
        .try_init();
}

pub fn init_test_module_logger(module: &str) {
    let _ = env_logger::Builder::new()
        .filter_module("tokio_reactor", LevelFilter::Info)
        .filter_module("tokio_core", LevelFilter::Info)
        .filter_module("hyper", LevelFilter::Info)
        .filter_module("cranelift_wasm", LevelFilter::Warn)
        .filter_module(module, LevelFilter::Debug)
        .filter(None, LevelFilter::Info)
        .try_init();
}

pub fn init_integration_logger() {
    let _ = env_logger::Builder::new()
        .filter(None, LevelFilter::Info)
        .filter(Some("actix_web"), LevelFilter::Warn)
        .try_init();
}

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub fn get_key_pair_from_seed(seed_string: &str) -> (PublicKey, SecretKey) {
    let mut seed: [u8; 32] = [b' '; 32];
    let len = ::std::cmp::min(32, seed_string.len());
    seed[..len].copy_from_slice(&seed_string.as_bytes()[..len]);

    let (public_key, secret_key) = keypair_from_seed(&Seed(seed));
    (PublicKey(public_key), SecretKey(secret_key))
}

pub fn get_public_key_from_seed(seed_string: &str) -> PublicKey {
    get_key_pair_from_seed(seed_string).0
}

pub fn get_bls_key_pair_from_seed(seed_string: &str) -> (BlsPublicKey, BlsSecretKey) {
    let mut rng = XorShiftRng::seed_from_u64(calculate_hash(&seed_string));
    let bls_secret_key = BlsSecretKey::generate_from_rng(&mut rng);
    (bls_secret_key.get_public_key(), bls_secret_key)
}

impl InMemorySigner {
    pub fn from_seed(account_id: &str, seed_string: &str) -> Self {
        let (public_key, secret_key) = get_key_pair_from_seed(seed_string);
        InMemorySigner { account_id: account_id.to_string(), public_key, secret_key }
    }
}

impl TransactionBody {
    pub fn sign(self, signer: &dyn EDSigner) -> SignedTransaction {
        let signature = signer.sign(self.get_hash().as_ref());
        SignedTransaction::new(signature, self, Some(signer.public_key()))
    }
}
