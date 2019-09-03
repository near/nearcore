#[macro_use]
extern crate bencher;

use std::collections::HashMap;
use std::sync::Arc;

use bencher::Bencher;
use borsh::{Deserializable, Serializable};
use chrono::Utc;

use near_crypto::{InMemorySigner, KeyType, PublicKey, Signature};
use near_primitives::account::Account;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, SignedTransaction, Transaction, TransferAction};
use near_primitives::types::MerkleHash;

fn create_transaction() -> SignedTransaction {
    let mut actions = vec![];
    for _ in 0..10 {
        actions.push(Action::Transfer(TransferAction { deposit: 1_000_000_000 }));
    }
    SignedTransaction::new(
        Signature::empty(KeyType::ED25519),
        Transaction {
            signer_id: "123213123123".to_string(),
            public_key: PublicKey::empty(KeyType::ED25519),
            nonce: 123,
            receiver_id: "1231231232131".to_string(),
            block_hash: Default::default(),
            actions,
        },
    )
}

fn create_block() -> Block {
    let transactions = (0..1000).map(|_| create_transaction()).collect::<Vec<_>>();
    let genesis = Block::genesis(MerkleHash::default(), Utc::now());
    let signer = Arc::new(InMemorySigner::from_random("".to_string(), KeyType::ED25519));
    Block::produce(
        &genesis.header,
        10,
        MerkleHash::default(),
        CryptoHash::default(),
        transactions,
        HashMap::default(),
        vec![],
        signer.clone(),
    )
}

fn create_account() -> Account {
    Account::new(0, CryptoHash::default(), 1_000)
}

fn serialize_tx(bench: &mut Bencher) {
    let t = create_transaction();
    bench.iter(|| {
        let bytes = t.try_to_vec().unwrap();
        assert!(bytes.len() > 0);
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
        assert!(bytes.len() > 0);
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
        assert!(bytes.len() > 0);
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

benchmark_group!(
    benches,
    serialize_tx,
    deserialize_tx,
    serialize_block,
    deserialize_block,
    serialize_account,
    deserialize_account
);
benchmark_main!(benches);
