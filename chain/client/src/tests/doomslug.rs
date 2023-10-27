use crate::test_utils::TestEnv;
use near_chain::{ChainGenesis, Provenance};
use near_crypto::KeyType;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::{Approval, ApprovalType};
use near_primitives::hash::CryptoHash;
use near_primitives::validator_signer::InMemoryValidatorSigner;

/// This file contains tests that test the interaction of client and doomslug, including how client handles approvals, etc.
/// It does not include the unit tests for the Doomslug class. That is located in chain/chain/src/doomslug.rs

// This tests the scenario that if the chain switch back and forth in between two forks, client code
// can process the skip messages correctly and send it to doomslug. Specifically, it tests the following
// case:
// existing chain looks like 0 - 1
//                             \ 2
// test that if the node receives Skip(2, 4), it can process it successfully.
#[test]
fn test_processing_skips_on_forks() {
    init_test_logger();

    let mut env =
        TestEnv::builder(ChainGenesis::test()).clients_count(2).validator_seats(2).build();
    let b1 = env.clients[1].produce_block(1).unwrap().unwrap();
    let b2 = env.clients[0].produce_block(2).unwrap().unwrap();
    assert_eq!(b1.header().prev_hash(), b2.header().prev_hash());
    env.process_block(1, b1, Provenance::NONE);
    env.process_block(1, b2, Provenance::NONE);
    let validator_signer =
        InMemoryValidatorSigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
    let approval = Approval::new(CryptoHash::default(), 1, 3, &validator_signer);
    env.clients[1].collect_block_approval(&approval, ApprovalType::SelfApproval);
    assert!(!env.clients[1].doomslug.approval_status_at_height(&3).approvals.is_empty());
}
