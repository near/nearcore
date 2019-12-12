use chrono::Duration;
use near_chain::chain::WEIGHT_MULTIPLIER;
use near_chain::test_utils::setup_with_validators;
use near_chain::{Block, ErrorKind, Provenance};
use near_crypto::{InMemorySigner, Signer};
use near_primitives::block::Approval;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::init_integration_logger;
use near_primitives::types::{BlockIndex, EpochId};
use near_primitives::utils::{from_timestamp, to_timestamp};
use std::{thread, time};

fn new_block(
    prev_block: &Block,
    height: BlockIndex,
    approvals: Vec<&str>,
    signer: &InMemorySigner,
    time: u64,
    time_delta: u128,
) -> Block {
    init_integration_logger();
    let num_approvals = approvals.len() as u128;
    let approvals = approvals
        .into_iter()
        .map(|x| Approval::new(prev_block.hash(), prev_block.hash(), signer, x.to_string()))
        .collect();
    let (epoch_id, next_epoch_id) = if prev_block.header.prev_hash == CryptoHash::default() {
        (prev_block.header.inner_lite.next_epoch_id.clone(), EpochId(prev_block.hash()))
    } else {
        (
            prev_block.header.inner_lite.epoch_id.clone(),
            prev_block.header.inner_lite.next_epoch_id.clone(),
        )
    };
    let weight_delta = std::cmp::max(1, num_approvals * WEIGHT_MULTIPLIER / 5);
    let mut block = Block::produce(
        &prev_block.header,
        height,
        prev_block.chunks.clone(),
        epoch_id,
        next_epoch_id,
        approvals,
        0,
        0,
        Some(0),
        vec![],
        vec![],
        signer,
        time_delta,
        weight_delta,
        0.into(),
        CryptoHash::default(),
        CryptoHash::default(),
        prev_block.header.inner_lite.next_bp_hash.clone(),
    );
    block.header.inner_lite.timestamp = time;
    block.header.init();
    block.header.signature = signer.sign(block.header.hash.as_ref());
    block
}

/// Testing various aspects of the fork choice rule.
///
/// The test first creates two chains in the following way:
///
///    GENESIS   <-- 400 --    honest1    <-- 10 -- honest2
///         ^
///         +--- 20 ---  adv1  <-- 20 --  adv2  ...  <-- 20 -- adv20
///
/// Where the number indicates the time delta. The honest blocks have 3/5 of approvals and adversarial
/// have 2/5 of approvals. Since in both networks the time from genesis to prev(tip) is 400, but the
/// first has more approvals per unit of time, we expect the first one to be the canonical, despite
/// the fact that the second has more blocks.
/// The test then advances the adversarial chain with a block that is two minutes from now, confirms
/// that is has higher weight that the honest tip, but makes sure that the client doesn't switch to
/// it yet (thus not letting adversaries to trick honest actors into choosing their chian by creating
/// blocks in the future).
/// The test also separately tests that the block that is more than two minutes in the future doesn't
/// get accepted
fn longer_adversarial_chain_common(reverse: bool) {
    let (mut chain, _, signers) = setup_with_validators(
        vec!["test0", "test1", "test2", "test3", "test4"].iter().map(|x| x.to_string()).collect(),
        1,
        1,
        1000,
        100,
    );
    let genesis = chain.get_block(&chain.genesis().hash()).unwrap().clone();

    let now = genesis.header.inner_lite.timestamp;
    let honest1 =
        new_block(&genesis, 1, vec!["test0", "test1", "test2"], &*signers[1], now + 400, 0);
    let honest2 =
        new_block(&honest1, 2, vec!["test0", "test1", "test2"], &*signers[2], now + 420, 400);

    let last_honest_hash = honest2.hash();

    let mut honest_blocks = vec![honest1, honest2];
    let mut all_blocks = vec![];

    if !reverse {
        all_blocks.append(&mut honest_blocks);
    }

    let mut last_block = &genesis;
    for i in 0..=20 {
        let adversarial = new_block(
            last_block,
            3 + i * 5,
            vec!["test3", "test4"],
            &*signers[3],
            now + 20 * (1 + i) as u64,
            if last_block == &genesis { 0 } else { 20 },
        );
        all_blocks.push(adversarial);
        last_block = &all_blocks.last().unwrap();
    }

    let last_adversarial_hash = last_block.hash();
    let one_more_adversarial_block = new_block(
        last_block,
        996,
        vec!["test3", "test4"],
        &*signers[996 % 5],
        to_timestamp(from_timestamp(now) + Duration::seconds(118)),
        20,
    );
    // We need the `and_one_more` because the weight of the block is the time delta between the two
    // *preceding* blocks, so for the large time delta of `one_more_adversarial_block` to take any
    // effect there must be one more block on top of it
    let and_one_more = new_block(
        &one_more_adversarial_block,
        997,
        vec!["test3", "test4"],
        &*signers[997 % 5],
        to_timestamp(from_timestamp(now) + Duration::seconds(119)),
        (one_more_adversarial_block.header.inner_lite.timestamp
            - last_block.header.inner_lite.timestamp) as u128,
    );
    // This block is just to test that blocks more than two minutes in the future cannot be accepted
    let too_far_adversarial_block = new_block(
        last_block,
        999,
        vec!["test3", "test4"],
        &*signers[999 % 5],
        to_timestamp(from_timestamp(now) + Duration::seconds(150)),
        20,
    );
    // This block is only few seconds into the future, so we can add it, make sure it doesn't become
    // the head, sleep the corresponding number of seconds, and add it again, this time expecting it
    // to become the head
    let block_close_future_1 = new_block(
        last_block,
        500,
        vec!["test3", "test4"],
        &*signers[500 % 5],
        to_timestamp(from_timestamp(now) + Duration::seconds(2)),
        20,
    );
    let block_close_future_2 = new_block(
        &block_close_future_1,
        501,
        vec!["test3", "test4"],
        &*signers[501 % 5],
        to_timestamp(from_timestamp(now) + Duration::seconds(2)) + 20,
        (block_close_future_1.header.inner_lite.timestamp - last_block.header.inner_lite.timestamp)
            as u128,
    );
    let block_close_future_3 = new_block(
        &block_close_future_1,
        502,
        vec!["test3", "test4"],
        &*signers[502 % 5],
        to_timestamp(from_timestamp(now) + Duration::seconds(2)) + 20,
        (block_close_future_1.header.inner_lite.timestamp - last_block.header.inner_lite.timestamp)
            as u128,
    );

    // The following two blocks test pro-rating the weight for blocks that have 3 approvals
    // Note that only having 3 approvals on the latter block matters (since the main contribution
    // to the weight will be that block weight multiplied by the former block time delta), so
    // intentionally leave the first of the two blocks with no approvals to make sure they don't
    // matter
    let block_with_three_approvals_in_the_future_1 = new_block(
        last_block,
        700,
        vec![],
        &*signers[700 % 5],
        to_timestamp(from_timestamp(now) + Duration::seconds(118)),
        20,
    );
    let block_with_three_approvals_in_the_future_2 = new_block(
        &block_with_three_approvals_in_the_future_1,
        701,
        vec!["test2", "test3", "test4"],
        &*signers[701 % 5],
        to_timestamp(from_timestamp(now) + Duration::seconds(119)),
        (block_with_three_approvals_in_the_future_1.header.inner_lite.timestamp
            - last_block.header.inner_lite.timestamp) as u128,
    );

    if reverse {
        all_blocks.append(&mut honest_blocks);
    }

    thread::sleep(time::Duration::from_nanos(420));

    for block in all_blocks {
        chain.process_block(&None, block, Provenance::NONE, |_| {}, |_| {}, |_| {}).unwrap();
    }

    let head_hash = chain.head().unwrap().last_block_hash;
    println!(
        "last honest: {:?}, last adversarial: {:?}, head: {:?}",
        last_honest_hash, last_adversarial_hash, head_hash
    );
    assert_eq!(head_hash, last_honest_hash);

    chain
        .process_block(&None, one_more_adversarial_block, Provenance::NONE, |_| {}, |_| {}, |_| {})
        .unwrap();
    chain.process_block(&None, and_one_more, Provenance::NONE, |_| {}, |_| {}, |_| {}).unwrap();
    match chain.process_block(
        &None,
        too_far_adversarial_block,
        Provenance::NONE,
        |_| {},
        |_| {},
        |_| {},
    ) {
        Ok(_) => {
            assert!(false);
        }
        Err(e) => {
            if let ErrorKind::InvalidBlockFutureTime(_) = e.kind() {
                // expected
            } else {
                println!("{:?}", e.kind());
                assert!(false);
            }
        }
    };
    let head_hash = chain.head().unwrap().last_block_hash;
    assert_eq!(head_hash, last_honest_hash);

    // Add the blocks in the close future, expect the head to still be at the last honest hash
    chain
        .process_block(&None, block_close_future_1, Provenance::NONE, |_| {}, |_| {}, |_| {})
        .unwrap();
    chain
        .process_block(&None, block_close_future_2, Provenance::NONE, |_| {}, |_| {}, |_| {})
        .unwrap();
    let head_hash = chain.head().unwrap().last_block_hash;
    assert_eq!(head_hash, last_honest_hash);

    // Sleep until the close future becomes past, and this time expect the head to change
    thread::sleep(time::Duration::from_secs(2));
    let block_close_future_3_hash = block_close_future_3.hash();
    chain
        .process_block(&None, block_close_future_3, Provenance::NONE, |_| {}, |_| {}, |_| {})
        .unwrap();
    let head_hash = chain.head().unwrap().last_block_hash;
    assert_eq!(head_hash, block_close_future_3_hash);

    // Pro-rating the weight should not affect blocks that actually have >1/2 approvals
    let block_with_three_approvals_in_the_future_2_hash =
        block_with_three_approvals_in_the_future_2.hash();
    chain
        .process_block(
            &None,
            block_with_three_approvals_in_the_future_1,
            Provenance::NONE,
            |_| {},
            |_| {},
            |_| {},
        )
        .unwrap();
    chain
        .process_block(
            &None,
            block_with_three_approvals_in_the_future_2,
            Provenance::NONE,
            |_| {},
            |_| {},
            |_| {},
        )
        .unwrap();
    let head_hash = chain.head().unwrap().last_block_hash;
    assert_eq!(head_hash, block_with_three_approvals_in_the_future_2_hash);
}

#[test]
fn longer_adversarial_chain1() {
    longer_adversarial_chain_common(false);
}

#[test]
fn longer_adversarial_chain2() {
    longer_adversarial_chain_common(true);
}
