use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use exonum_sodiumoxide::crypto::sign::ed25519::{keypair_from_seed, Seed};
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;

use crate::aggregate_signature::{BlsPublicKey, BlsSecretKey};
use crate::beacon::SignedBeaconBlock;
use crate::block_traits::{SignedBlock, SignedHeader};
use crate::chain::{SignedShardBlock, SignedShardBlockHeader};
use crate::hash::CryptoHash;
use crate::signature::{PublicKey, SecretKey};
use crate::signer::InMemorySigner;

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

pub fn get_bls_key_pair_from_seed(seed_string: &str) -> (BlsPublicKey, BlsSecretKey) {
    let mut rng = XorShiftRng::seed_from_u64(calculate_hash(&seed_string));
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

pub trait TestSignedBlock: SignedBlock {
    fn sign_all(&mut self, signers: &Vec<Arc<InMemorySigner>>) {
        for (i, signer) in signers.iter().enumerate() {
            self.add_signature(&self.sign(signer.clone()), i);
        }
    }
}

impl SignedShardBlock {
    pub fn empty(prev: &SignedShardBlockHeader) -> Self {
        SignedShardBlock::new(
            prev.shard_id(),
            prev.index() + 1,
            prev.hash,
            prev.merkle_root_state(),
            vec![],
            vec![],
            CryptoHash::default(),
        )
    }
}

impl TestSignedBlock for SignedShardBlock {}
impl TestSignedBlock for SignedBeaconBlock {}