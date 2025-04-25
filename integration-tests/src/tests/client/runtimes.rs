//! Client is responsible for tracking the chain, chunks, and producing them when needed.
//! This client works completely synchronously and must be operated by some async actor outside.

use near_chain_configs::Genesis;
use near_crypto::KeyType;
use near_network::test_utils::MockPeerManagerAdapter;
use near_primitives::block::{Approval, ApprovalInner};
use near_primitives::block_header::ApprovalType;
use near_primitives::hash::hash;
use near_primitives::network::PeerId;
use near_primitives::test_utils::create_test_signer;
use near_primitives::validator_signer::InMemoryValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use std::collections::HashMap;
use std::sync::Arc;

use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;

#[test]
fn test_pending_approvals() {
    let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let signer = create_test_signer("test0");
    let parent_hash = hash(&[1]);
    let approval = Approval::new(parent_hash, 0, 1, &signer);
    let peer_id = PeerId::random();
    let client_signer = env.clients[0].validator_signer.get();
    env.clients[0].collect_block_approval(
        &approval,
        ApprovalType::PeerApproval(peer_id.clone()),
        &client_signer,
    );
    let approvals = env.clients[0].pending_approvals.pop(&ApprovalInner::Endorsement(parent_hash));
    let expected =
        vec![("test0".parse().unwrap(), (approval, ApprovalType::PeerApproval(peer_id)))]
            .into_iter()
            .collect::<HashMap<_, _>>();
    assert_eq!(approvals, Some(expected));
}

#[test]
fn test_invalid_approvals() {
    let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let mut env = TestEnv::builder(&genesis.config)
        .nightshade_runtimes(&genesis)
        .network_adapters(vec![network_adapter])
        .build();
    let signer = create_test_signer("random");
    let parent_hash = hash(&[1]);
    // Approval not from a validator. Should be dropped
    let approval = Approval::new(parent_hash, 1, 3, &signer);
    let peer_id = PeerId::random();
    let client_signer = env.clients[0].validator_signer.get();
    env.clients[0].collect_block_approval(
        &approval,
        ApprovalType::PeerApproval(peer_id.clone()),
        &client_signer,
    );
    assert_eq!(env.clients[0].pending_approvals.len(), 0);
    // Approval with invalid signature. Should be dropped
    let signer =
        InMemoryValidatorSigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "random");
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let approval = Approval::new(genesis_hash, 0, 1, &signer);
    env.clients[0].collect_block_approval(
        &approval,
        ApprovalType::PeerApproval(peer_id),
        &client_signer,
    );
    assert_eq!(env.clients[0].pending_approvals.len(), 0);
}

#[test]
fn test_cap_max_gas_price() {
    use near_chain::Provenance;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let epoch_length = 5;
    genesis.config.min_gas_price = 1_000;
    genesis.config.max_gas_price = 1_000_000;
    genesis.config.protocol_version = PROTOCOL_VERSION;
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();

    for i in 1..epoch_length {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block, Provenance::PRODUCED);
    }

    let min_gas_price = env.clients[0].chain.block_economics_config.min_gas_price();
    let max_gas_price = env.clients[0].chain.block_economics_config.max_gas_price();
    assert!(max_gas_price <= 20 * min_gas_price);
}
