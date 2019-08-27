use std::collections::HashMap;

use near_chain::test_utils::{setup, setup_with_tx_validity_period};
use near_chain::{Block, ErrorKind, Provenance};
use near_crypto::{KeyType, Signature, Signer};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::test_utils::init_test_logger;
use near_primitives::transaction::{SignedTransaction, Transaction};
use near_primitives::types::MerkleHash;

#[test]
fn empty_chain() {
    init_test_logger();
    let (chain, _, _) = setup();
    assert_eq!(chain.head().unwrap().height, 0);
}

#[test]
fn build_chain() {
    init_test_logger();
    let (mut chain, _, signer) = setup();
    for i in 0..4 {
        let prev = chain.head_header().unwrap();
        let block = Block::empty(&prev, signer.clone());
        let tip = chain.process_block(block, Provenance::PRODUCED, |_, _, _| {}).unwrap();
        assert_eq!(tip.unwrap().height, i + 1);
    }
    assert_eq!(chain.head().unwrap().height, 4);
}

#[test]
fn build_chain_with_orhpans() {
    init_test_logger();
    let (mut chain, _, signer) = setup();
    let mut blocks = vec![chain.get_block(&chain.genesis().hash()).unwrap().clone()];
    for i in 1..4 {
        let block = Block::empty(&blocks[i - 1].header, signer.clone());
        blocks.push(block);
    }
    let block = Block::produce(
        &blocks[blocks.len() - 1].header,
        10,
        blocks[blocks.len() - 1].header.inner.prev_state_root,
        blocks[blocks.len() - 1].header.inner.epoch_hash,
        vec![],
        HashMap::default(),
        vec![],
        signer.clone(),
    );
    assert_eq!(
        chain.process_block(block, Provenance::PRODUCED, |_, _, _| {}).unwrap_err().kind(),
        ErrorKind::Orphan
    );
    assert_eq!(
        chain
            .process_block(blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Orphan
    );
    assert_eq!(
        chain
            .process_block(blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Orphan
    );
    let res = chain.process_block(blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {});
    assert_eq!(res.unwrap().unwrap().height, 10);
    assert_eq!(
        chain
            .process_block(blocks.pop().unwrap(), Provenance::PRODUCED, |_, _, _| {})
            .unwrap_err()
            .kind(),
        ErrorKind::Unfit("already known in store".to_string())
    );
}

#[test]
fn build_chain_with_skips_and_forks() {
    init_test_logger();
    let (mut chain, _, signer) = setup();
    let b1 = Block::empty(chain.genesis(), signer.clone());
    let b2 = Block::produce(
        chain.genesis(),
        2,
        MerkleHash::default(),
        CryptoHash::default(),
        vec![],
        HashMap::default(),
        vec![],
        signer.clone(),
    );
    let b3 = Block::empty(&b1.header, signer.clone());
    let b4 = Block::produce(
        &b2.header,
        4,
        MerkleHash::default(),
        CryptoHash::default(),
        vec![],
        HashMap::default(),
        vec![],
        signer.clone(),
    );
    let b5 = Block::empty(&b4.header, signer);
    assert!(chain.process_block(b1, Provenance::PRODUCED, |_, _, _| {}).is_ok());
    assert!(chain.process_block(b2, Provenance::PRODUCED, |_, _, _| {}).is_ok());
    assert!(chain.process_block(b3, Provenance::PRODUCED, |_, _, _| {}).is_ok());
    assert!(chain.process_block(b4, Provenance::PRODUCED, |_, _, _| {}).is_ok());
    assert!(chain.process_block(b5, Provenance::PRODUCED, |_, _, _| {}).is_ok());
    assert!(chain.get_header_by_height(1).is_err());
    assert_eq!(chain.get_header_by_height(5).unwrap().inner.height, 5);
}

#[test]
fn test_apply_expired_tx() {
    init_test_logger();
    let (mut chain, _, signer) = setup_with_tx_validity_period(0);
    let b1 = Block::empty(chain.genesis(), signer.clone());
    let tx = SignedTransaction::new(
        Signature::empty(KeyType::ED25519),
        Transaction {
            signer_id: "".to_string(),
            public_key: signer.public_key(),
            nonce: 0,
            receiver_id: "".to_string(),
            block_hash: b1.hash(),
            actions: vec![],
        },
    );
    let b2 = Block::produce(
        chain.genesis(),
        2,
        MerkleHash::default(),
        CryptoHash::default(),
        vec![tx],
        HashMap::default(),
        vec![],
        signer.clone(),
    );
    assert!(chain.process_block(b1, Provenance::PRODUCED, |_, _, _| {}).is_ok());
    assert!(chain.process_block(b2, Provenance::PRODUCED, |_, _, _| {}).is_err());
}

#[test]
fn test_tx_wrong_fork() {
    init_test_logger();
    let (mut chain, _, signer) = setup();
    let b1 = Block::empty(chain.genesis(), signer.clone());
    let tx = SignedTransaction::new(
        Signature::empty(KeyType::ED25519),
        Transaction {
            signer_id: "".to_string(),
            public_key: signer.public_key(),
            nonce: 0,
            receiver_id: "".to_string(),
            block_hash: hash(&[2]),
            actions: vec![],
        },
    );
    let b2 = Block::produce(
        chain.genesis(),
        2,
        MerkleHash::default(),
        CryptoHash::default(),
        vec![tx],
        HashMap::default(),
        vec![],
        signer.clone(),
    );
    assert!(chain.process_block(b1, Provenance::PRODUCED, |_, _, _| {}).is_ok());
    assert!(chain.process_block(b2, Provenance::PRODUCED, |_, _, _| {}).is_err());
}
