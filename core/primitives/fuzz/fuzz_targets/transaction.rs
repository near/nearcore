#![no_main]

use near_primitives::transaction::{Transaction, SignedTransaction};
use near_primitives::borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{KeyType, Signature};


libfuzzer_sys::fuzz_target!(|data: Transaction| {
    let signed_tx = SignedTransaction::new(Signature::empty(KeyType::ED25519), data.clone());

    let mut serialized = vec![];
    let _ = signed_tx.serialize(&mut serialized).unwrap();
    let deserialized_tx = SignedTransaction::try_from_slice(&serialized).unwrap();

    // Assert that the deserialized transaction matches the original
    assert_eq!(data, deserialized_tx.transaction);
});

