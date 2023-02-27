#[macro_use]
extern crate bencher;

use bencher::{black_box, Bencher};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::static_clock::StaticClock;

use near_crypto::{KeyType, PublicKey, Signature};
use near_primitives::account::Account;
use near_primitives::block::{genesis_chunks, Block};
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::combine_hash;
use near_primitives::test_utils::account_new;
use near_primitives::transaction::{Action, SignedTransaction, Transaction, TransferAction};
use near_primitives::types::{EpochId, StateRoot};
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives_core::types::MerkleHash;
use num_rational::Rational32;

fn create_transaction() -> SignedTransaction {
    let mut actions = vec![];
    for _ in 0..10 {
        actions.push(Action::Transfer(TransferAction { deposit: 1_000_000_000 }));
    }
    SignedTransaction::new(
        Signature::empty(KeyType::ED25519),
        Transaction {
            signer_id: "123213123123".parse().unwrap(),
            public_key: PublicKey::empty(KeyType::ED25519),
            nonce: 123,
            receiver_id: "1231231232131".parse().unwrap(),
            block_hash: Default::default(),
            actions,
        },
    )
}

fn create_block() -> Block {
    let genesis_chunks = genesis_chunks(vec![StateRoot::new()], 1, 1_000, 0, PROTOCOL_VERSION);
    let genesis = Block::genesis(
        PROTOCOL_VERSION,
        genesis_chunks.into_iter().map(|chunk| chunk.take_header()).collect(),
        StaticClock::utc(),
        0,
        1_000,
        1_000,
        CryptoHash::default(),
    );
    let signer = InMemoryValidatorSigner::from_random("test".parse().unwrap(), KeyType::ED25519);
    Block::produce(
        PROTOCOL_VERSION,
        PROTOCOL_VERSION,
        genesis.header(),
        10,
        genesis.header().block_ordinal() + 1,
        vec![genesis.chunks()[0].clone()],
        EpochId::default(),
        EpochId::default(),
        None,
        vec![],
        Rational32::from_integer(0),
        0,
        0,
        Some(0),
        vec![],
        vec![],
        &signer,
        CryptoHash::default(),
        CryptoHash::default(),
        None,
    )
}

fn create_account() -> Account {
    account_new(0, CryptoHash::default())
}

fn serialize_tx(bench: &mut Bencher) {
    let t = create_transaction();
    bench.iter(|| {
        let bytes = t.try_to_vec().unwrap();
        assert!(!bytes.is_empty());
    });
}

fn deserialize_tx(bench: &mut Bencher) {
    let t = create_transaction();
    let bytes = t.try_to_vec().unwrap();
    bench.iter(|| {
        let nt = SignedTransaction::try_from_slice(&bytes).unwrap();
        assert_eq!(nt, t);
    });
}

fn serialize_block(bench: &mut Bencher) {
    let b = create_block();
    bench.iter(|| {
        let bytes = b.try_to_vec().unwrap();
        assert!(!bytes.is_empty());
    });
}

fn deserialize_block(bench: &mut Bencher) {
    let b = create_block();
    let bytes = b.try_to_vec().unwrap();
    bench.iter(|| {
        let nb = Block::try_from_slice(&bytes).unwrap();
        assert_eq!(nb, b);
    });
}

fn serialize_account(bench: &mut Bencher) {
    let acc = create_account();
    bench.iter(|| {
        let bytes = acc.try_to_vec().unwrap();
        assert!(!bytes.is_empty());
    });
}

fn deserialize_account(bench: &mut Bencher) {
    let acc = create_account();
    let bytes = acc.try_to_vec().unwrap();
    bench.iter(|| {
        let nacc = Account::try_from_slice(&bytes).unwrap();
        assert_eq!(nacc, acc);
    });
}

fn combine_hash_bench(bench: &mut Bencher) {
    let a = MerkleHash::default();
    let b = MerkleHash::default();
    bench.iter(|| {
        let res = combine_hash(black_box(&a), black_box(&b));
        black_box(res)
    });
}

benchmark_group!(
    benches,
    serialize_tx,
    deserialize_tx,
    serialize_block,
    deserialize_block,
    serialize_account,
    deserialize_account,
    combine_hash_bench,
);
benchmark_main!(benches);
