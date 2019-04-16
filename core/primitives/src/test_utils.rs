use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use exonum_sodiumoxide::crypto::sign::ed25519::{keypair_from_seed, Seed};
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;

use crate::beacon::SignedBeaconBlock;
use crate::block_traits::{SignedBlock, SignedHeader};
use crate::chain::{SignedShardBlock, SignedShardBlockHeader, ChainPayload, ReceiptBlock, Snapshot};
use crate::crypto::aggregate_signature::{BlsPublicKey, BlsSecretKey};
use crate::crypto::signature::{PublicKey, SecretKey};
use crate::crypto::signer::{AccountSigner, BLSSigner, EDSigner, InMemorySigner};
use crate::hash::CryptoHash;
use crate::transaction::{SignedTransaction, TransactionBody};
use crate::types::{AccountId, AuthorityId, AuthorityStake};
use crate::nightshade::{Proof, BareState, BlockProposal};
use crate::crypto::group_signature::GroupSignature;

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
    fn sign_all<T: BLSSigner + AccountSigner>(
        &mut self,
        authorities: &HashMap<AuthorityId, AuthorityStake>,
        signers: &[Arc<T>],
    ) {
        let signer_map: HashMap<AccountId, Arc<T>> =
            signers.iter().map(|s| (s.account_id(), s.clone())).collect();
        for (i, authority_stake) in authorities.iter() {
            self.add_signature(&self.sign(&*signer_map[&authority_stake.account_id]), *i);
        }
    }
}

pub fn forge_proof<T: BLSSigner + AccountSigner, RefT: AsRef<T>>(
    payload: &ChainPayload,
    authorities: &HashMap<AuthorityId, AuthorityStake>,
    signers: &[RefT],
) -> Proof {
    let transactions: Vec<_> = payload.transactions.iter().map(SignedTransaction::get_hash).collect();
    let receipts: Vec<_> = payload.receipts.iter().map(ReceiptBlock::get_hash).collect();
    let hash = Snapshot::new(transactions, receipts).get_hash();
    let bare_state = BareState {
        primary_confidence: 2,
        endorses: BlockProposal { author: 0, hash },
        secondary_confidence: 0
    };
    let mut group_signature = GroupSignature::default();
    let signer_map: HashMap<AccountId, &T> =
        signers.iter().map(|s| (s.as_ref().account_id(), s.as_ref())).collect();
    for (i, authority_stake) in authorities.iter() {
        let signer = &*signer_map[&authority_stake.account_id];
        let partial_signature = signer.bls_sign(&bare_state.bs_encode());
        group_signature.add_signature(&partial_signature, *i);
    }
    Proof {
        bare_state,
        signature: group_signature
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

impl TransactionBody {
    pub fn sign(self, signer: &EDSigner) -> SignedTransaction {
        let signature = signer.sign(self.get_hash().as_ref());
        SignedTransaction::new(signature, self)
    }
}
