use crate::test_utils::setup;
use crate::{Block, Chain, ChainStoreAccess};
use near_async::time::Clock;
use near_primitives::errors::{DepositCostFailureReason, InvalidTxError};
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::TestBlockBuilder;
use near_primitives::transaction::{ExecutionOutcome, ExecutionOutcomeWithProof, ExecutionStatus};
use near_primitives::types::{Balance, ShardId};
use near_store::adapter::StoreUpdateAdapter;
use std::sync::Arc;

/// A chain of `b1`, `b2` and `b3` on top of genesis, and a block at height 3 on top of `b1`.
struct Blocks {
    chain: Chain,
    b1: Arc<Block>,
    b2: Arc<Block>,
    b3: Arc<Block>,
    fork: Arc<Block>,
}

fn build_blocks() -> Blocks {
    let clock = Clock::real();
    let (mut chain, _, _, signer) = setup(clock.clone());
    let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();
    let b1 = TestBlockBuilder::from_prev_block(clock.clone(), &genesis, signer.clone()).build();
    let b2 = TestBlockBuilder::from_prev_block(clock.clone(), &b1, signer.clone()).build();
    let b3 = TestBlockBuilder::from_prev_block(clock.clone(), &b2, signer.clone()).build();
    let fork = TestBlockBuilder::from_prev_block(clock, &b1, signer).height(3).build();
    for block in [&b1, &b2, &b3, &fork] {
        chain.process_block_test(block.clone()).unwrap();
    }
    assert_eq!(chain.head().unwrap().last_block_hash, *b3.hash());
    Blocks { chain, b1, b2, b3, fork }
}

fn save_outcome(chain: &Chain, block: &Block, id: CryptoHash, status: ExecutionStatus) {
    let outcome = ExecutionOutcome { status, ..Default::default() };
    let outcome_with_proof = ExecutionOutcomeWithProof { proof: vec![], outcome };
    let mut store_update = chain.chain_store().store().store_update();
    store_update.chain_store_update().set_outcomes_with_proofs(
        block.hash(),
        ShardId::new(0),
        &[(id, outcome_with_proof)],
    );
    store_update.commit();
}

#[test]
fn get_execution_outcome_returns_the_inclusion_that_used_the_nonce_over_an_earlier_rejection() {
    let Blocks { chain, b1, b2, b3, .. } = build_blocks();
    let success = ExecutionStatus::SuccessReceiptId(CryptoHash::hash_bytes(b"receipt"));
    let deposit_failed = ExecutionStatus::Failure(
        InvalidTxError::NotEnoughBalanceForDeposit {
            signer_id: "test".parse().unwrap(),
            balance: Balance::ZERO,
            cost: Balance::from_yoctonear(1),
            reason: DepositCostFailureReason::NotEnoughBalance,
        }
        .into(),
    );
    let invalid_nonce =
        ExecutionStatus::Failure(InvalidTxError::InvalidNonce { tx_nonce: 1, ak_nonce: 1 }.into());
    for (name, used_nonce) in [("succeeded", success), ("deposit failed", deposit_failed)] {
        let id = CryptoHash::hash_bytes(name.as_bytes());
        save_outcome(&chain, &b1, id, invalid_nonce.clone());
        save_outcome(&chain, &b2, id, used_nonce.clone());
        save_outcome(&chain, &b3, id, invalid_nonce.clone());

        let outcome = chain.get_execution_outcome(&id).unwrap();
        assert_eq!(outcome.block_hash, *b2.hash());
        assert_eq!(outcome.outcome_with_id.outcome.status, used_nonce);
    }
}

#[test]
fn get_execution_outcome_returns_the_earliest_rejection_when_no_inclusion_used_the_nonce() {
    let Blocks { chain, b2, b3, .. } = build_blocks();
    let id = CryptoHash::hash_bytes(b"rejected");
    save_outcome(
        &chain,
        &b3,
        id,
        ExecutionStatus::Failure(InvalidTxError::InvalidNonce { tx_nonce: 1, ak_nonce: 1 }.into()),
    );
    let expired = ExecutionStatus::Failure(InvalidTxError::Expired.into());
    save_outcome(&chain, &b2, id, expired.clone());

    let outcome = chain.get_execution_outcome(&id).unwrap();
    assert_eq!(outcome.block_hash, *b2.hash());
    assert_eq!(outcome.outcome_with_id.outcome.status, expired);
}

#[test]
fn get_execution_outcome_skips_an_inclusion_that_used_the_nonce_off_the_canonical_chain() {
    let Blocks { chain, b3, fork, .. } = build_blocks();
    let id = CryptoHash::hash_bytes(b"forked");
    let success = ExecutionStatus::SuccessReceiptId(CryptoHash::hash_bytes(b"receipt"));
    save_outcome(&chain, &fork, id, success);
    save_outcome(
        &chain,
        &b3,
        id,
        ExecutionStatus::Failure(InvalidTxError::InvalidNonce { tx_nonce: 1, ak_nonce: 1 }.into()),
    );

    let outcome = chain.get_execution_outcome(&id).unwrap();
    assert_eq!(outcome.block_hash, *b3.hash());
}
