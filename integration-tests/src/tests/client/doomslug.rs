use near_chain::Provenance;
use near_chain_configs::Genesis;
use near_crypto::KeyType;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::{Approval, ApprovalType};
use near_primitives::hash::CryptoHash;
use near_primitives::validator_signer::InMemoryValidatorSigner;

use crate::env::test_env::TestEnv;

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

    // We don't want to mock epoch manager and for forks we want to pick block heights that
    // correspond to different block producers.
    let first_fork_heigh = 1;
    let second_fork_heigh = 3;
    let genesis =
        Genesis::test(["test0", "test1"].into_iter().map(|acc| acc.parse().unwrap()).collect(), 2);
    let mut env =
        TestEnv::builder_from_genesis(&genesis).clients_count(2).validator_seats(2).build();
    let b1 = env.clients[1].produce_block(first_fork_heigh).unwrap().unwrap();
    let b2 = env.clients[0].produce_block(second_fork_heigh).unwrap().unwrap();
    assert_eq!(b1.header().prev_hash(), b2.header().prev_hash());
    env.process_block(1, b1, Provenance::NONE);
    env.process_block(1, b2, Provenance::NONE);
    let validator_signer =
        InMemoryValidatorSigner::from_seed("test1".parse().unwrap(), KeyType::ED25519, "test1");
    let approval =
        Approval::new(CryptoHash::default(), 1, second_fork_heigh + 1, &validator_signer);
    let client_signer = env.clients[1].validator_signer.get();
    env.clients[1].collect_block_approval(&approval, ApprovalType::SelfApproval, &client_signer);
    assert!(
        !env.clients[1]
            .doomslug
            .approval_status_at_height(&(second_fork_heigh + 1))
            .approvals
            .is_empty()
    );
}
