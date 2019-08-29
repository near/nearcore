use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use exonum_sodiumoxide::crypto::sign::ed25519::{keypair_from_seed, Seed};
use log::LevelFilter;
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;

use crate::block::Block;
use crate::crypto::aggregate_signature::{BlsPublicKey, BlsSecretKey};
use crate::crypto::signature::{PublicKey, SecretKey, Signature};
use crate::crypto::signer::{EDSigner, InMemorySigner};
use crate::transaction::{SignedTransaction, TransactionBody};
use crate::types::{BlockIndex, EpochId};
use lazy_static::lazy_static;

lazy_static! {
    static ref HEAVY_TESTS_LOCK: Mutex<()> = Mutex::new(());
}

pub fn heavy_test<F>(f: F)
where
    F: FnOnce() -> (),
{
    let _guard = HEAVY_TESTS_LOCK.lock();
    f();
}

pub fn init_test_logger() {
    let _ = env_logger::Builder::new()
        .filter_module("tokio_reactor", LevelFilter::Info)
        .filter_module("tokio_core", LevelFilter::Info)
        .filter_module("hyper", LevelFilter::Info)
        .filter(None, LevelFilter::Info)
        .try_init();
}

pub fn init_test_module_logger(module: &str) {
    let _ = env_logger::Builder::new()
        .filter_module("tokio_reactor", LevelFilter::Info)
        .filter_module("tokio_core", LevelFilter::Info)
        .filter_module("hyper", LevelFilter::Info)
        .filter_module("cranelift_wasm", LevelFilter::Warn)
        .filter_module(module, LevelFilter::Info)
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

impl Block {
    pub fn empty_with_epoch(
        prev: &Block,
        height: BlockIndex,
        signer: Arc<dyn EDSigner>,
        epoch: EpochId,
    ) -> Self {
        Self::empty_with_approvals(prev, height, HashMap::default(), signer, epoch)
    }

    pub fn empty_with_height(prev: &Block, height: BlockIndex, signer: Arc<dyn EDSigner>) -> Self {
        Self::empty_with_epoch(prev, height, signer, prev.header.epoch_id.clone())
    }

    pub fn empty(prev: &Block, signer: Arc<dyn EDSigner>) -> Self {
        Self::empty_with_height(prev, prev.header.height + 1, signer)
    }

    /// This is not suppose to be used outside of chain tests, because this doesn't refer to correct chunks.
    /// Done because chain tests don't have a good way to store chunks right now.
    pub fn empty_with_approvals(
        prev: &Block,
        height: BlockIndex,
        approvals: HashMap<usize, Signature>,
        signer: Arc<dyn EDSigner>,
        epoch: EpochId,
    ) -> Self {
        Block::produce(
            &prev.header,
            height,
            prev.chunks.clone(),
            epoch,
            vec![],
            approvals,
            0,
            0,
            signer,
        )
    }
}
