use crate::doomslug::ChunksReadiness;
use crate::{Doomslug, DoomslugThresholdMode};
use near_async::time::{Duration, FakeClock, Instant, Utc};
use near_chain_configs::MutableConfigValue;
use near_crypto::{KeyType, SecretKey};
use near_primitives::block::Approval;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{ApprovalStake, Balance, BlockHeight};
use num_rational::Rational32;
use rand::{Rng, thread_rng};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use time::ext::InstantExt as _;

fn block_hash(height: BlockHeight, ord: usize) -> CryptoHash {
    hash(([height.to_le_bytes(), ord.to_le_bytes()].concat()).as_ref())
}

fn get_msg_delivery_time(now: Instant, gst: Instant, delta: Duration) -> Instant {
    std::cmp::max(now, gst)
        + Duration::milliseconds(thread_rng().gen_range(0..delta.whole_milliseconds() as i64) as i64)
}

/// The earliest time at which a message gets delivered or a doomslug timer fires.
fn next_event_time(
    approval_queue: &[(Approval, Instant)],
    block_queue: &[(BlockHeight, usize, BlockHeight, Instant, CryptoHash)],
    doomslugs: &[Doomslug],
) -> Instant {
    let approval_times = approval_queue.iter().map(|approval| approval.1);
    let block_times = block_queue.iter().map(|block| block.3);
    let timer_deadlines = doomslugs.iter().map(|ds| ds.next_timer_deadline());
    let message_times = approval_times.chain(block_times);
    let event_times = message_times.chain(timer_deadlines);
    event_times.min().unwrap()
}

/// Number of ticks (at least one) to advance the clock by to reach `deadline`.
fn num_ticks_until(now: Instant, deadline: Instant, tick: Duration) -> u32 {
    let remaining = deadline.signed_duration_since(now);
    let remaining_nanos = remaining.whole_nanoseconds();
    let tick_nanos = tick.whole_nanoseconds();
    // Round up, the deadline is reached at the first tick that is not before it.
    let rounded_up_nanos = remaining_nanos + tick_nanos - 1;
    let ticks = rounded_up_nanos.div_euclid(tick_nanos);
    let ticks = ticks.clamp(1, u32::MAX as i128);
    ticks as u32
}

/// In the liveness proof we rely on timers always being within delta from each other.
/// Validates that assumption.
fn assert_timers_in_sync(doomslugs: &[Doomslug], now: Instant, delta: Duration) {
    // Only makes sense for timers that are more than delta in the past, since for more
    // recent timers the other participant's start time might be in the future
    let timer_starts = doomslugs
        .iter()
        .map(|ds| ds.get_timer_start())
        .filter(|timer_start| now - *timer_start >= delta)
        .collect::<Vec<_>>();
    let Some(min_timer_start) = timer_starts.iter().min() else {
        return;
    };
    let Some(max_timer_start) = timer_starts.iter().max() else {
        return;
    };
    let timers_spread = *max_timer_start - *min_timer_start;
    assert!(timers_spread <= delta);
}

/// Runs a single iteration of a fuzz test given specific time until global stabilization and
/// the max delay on messages.
/// Returns amount of time it took to produce a doomslug final block at height 50, as well as the
/// largest height encountered
///
/// # Arguments
/// * `time_to_gst` - number of milliseconds before global stabilization time
/// * `delta`       - max message delay
/// * `height_goal` - the appearance of a block at this (or higher) height with finality
///                   will end the test
fn one_iter(
    time_to_gst: Duration,
    delta: Duration,
    height_goal: BlockHeight,
) -> (Duration, BlockHeight) {
    let account_ids = ["test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8"];
    let stakes = account_ids
        .iter()
        .map(|account_id| ApprovalStake {
            account_id: account_id.parse().unwrap(),
            stake_this_epoch: Balance::from_yoctonear(1),
            stake_next_epoch: Balance::from_yoctonear(1),
            public_key: SecretKey::from_seed(KeyType::ED25519, account_id).public_key(),
        })
        .collect::<Vec<_>>();
    let signers = account_ids
        .iter()
        .map(|account_id| Some(Arc::new(create_test_signer(account_id))))
        .collect::<Vec<_>>();
    let clock = FakeClock::new(Utc::UNIX_EPOCH);
    let mut doomslugs: Vec<_> = std::iter::repeat_with(|| {
        Doomslug::new(
            clock.clock(),
            0,
            MutableConfigValue::new(Duration::milliseconds(200), "endorsement_delay"),
            MutableConfigValue::new(Duration::milliseconds(1000), "min_delay"),
            // some arbitrary number larger than delta * 6
            MutableConfigValue::new(delta * 20, "max_delay"),
            MutableConfigValue::new(Rational32::new(1, 3), "chunk_wait_mult"),
            DoomslugThresholdMode::TwoThirds,
        )
    })
    .take(signers.len())
    .collect();

    let started = clock.now();
    let gst = clock.now() + time_to_gst;
    let mut approval_queue: Vec<(Approval, Instant)> = vec![];
    let mut block_queue: Vec<(BlockHeight, usize, BlockHeight, Instant, CryptoHash)> = vec![];
    let mut largest_produced_height: BlockHeight = 1;
    let mut chain_lengths = HashMap::new();
    let mut hash_to_block_info: HashMap<CryptoHash, (BlockHeight, BlockHeight, CryptoHash)> =
        HashMap::new();
    let mut hash_to_prev_hash: HashMap<CryptoHash, CryptoHash> = HashMap::new();

    let mut blocks_with_finality: Vec<(CryptoHash, BlockHeight)> = vec![];

    chain_lengths.insert(block_hash(1, 0), 1);

    for ds in &mut doomslugs {
        ds.set_tip(block_hash(1, 0), 1, 1);
        hash_to_block_info.insert(block_hash(1, 0), (1, 1, block_hash(1, 0)));
    }

    let tick = Duration::milliseconds(25);
    let mut is_done = false;
    while !is_done {
        // Nothing happens until the next message delivery or the next doomslug timer, so instead
        // of stepping through the idle ticks one by one, jump right before the first tick that
        // has something to do. The timers do not change during the idle ticks, so validating the
        // timers once at the last of them covers all of them.
        let next_event = next_event_time(&approval_queue, &block_queue, &doomslugs);
        let ticks_until_next_event = num_ticks_until(clock.now(), next_event, tick);
        let idle_ticks = ticks_until_next_event - 1;
        if idle_ticks > 0 {
            clock.advance(tick * idle_ticks);
            assert_timers_in_sync(&doomslugs, clock.now(), delta);
        }

        clock.advance(tick);
        let mut new_approval_queue = vec![];
        let mut new_block_queue = vec![];

        // 1. Process approvals
        for approval in approval_queue {
            if approval.1 > clock.now() {
                new_approval_queue.push(approval);
            } else {
                let me = (approval.0.target_height % 8) as usize;

                // Make test1 and test2 be offline and never send approvals
                if matches!(approval.0.account_id.as_ref(), "test1" | "test2") {
                    continue;
                }

                // Generally make 20% of the remaining approvals to drop
                if thread_rng().gen_range(0..10) < 2 {
                    continue;
                }

                doomslugs[me].on_approval_message(&approval.0, &stakes);
            }
        }
        approval_queue = new_approval_queue;

        // 2. Process blocks
        for block in block_queue {
            if block.3 > clock.now() {
                new_block_queue.push(block);
            } else {
                let ds = &mut doomslugs[block.1 as usize];
                if block.0 as BlockHeight > ds.get_tip().1 {
                    // Accept all the blocks from the tip till this block
                    let mut block_infos = vec![(block.0, block.2, block.4)];
                    for block_index in 0..50 {
                        if block_index == 49 {
                            assert!(false);
                        }

                        let last_block = block_infos.last().unwrap();
                        let prev_block_info = hash_to_block_info
                            .get(&hash_to_prev_hash.get(&last_block.2).unwrap())
                            .unwrap();

                        if prev_block_info.0 as BlockHeight <= ds.get_tip().1 {
                            break;
                        }
                        block_infos.push(*prev_block_info);
                    }

                    for block_info in block_infos.into_iter().rev() {
                        if block_info.0 > ds.get_tip().1 {
                            ds.set_tip(block_info.2, block_info.0 as BlockHeight, block_info.1);
                        }
                    }
                }
            }
        }
        block_queue = new_block_queue;

        // 3. Process timers
        for (i, ds) in doomslugs.iter_mut().enumerate() {
            for approval in ds.process_timer(&signers[i]) {
                approval_queue.push((approval, get_msg_delivery_time(clock.now(), gst, delta)));
            }
        }

        // 4. Produce blocks
        'outer: for (bp_ord, ds) in doomslugs.iter_mut().enumerate() {
            for target_height in (ds.get_tip().1 + 1)..=ds.get_largest_height_crossing_threshold() {
                if ds.ready_to_produce_block(
                    target_height,
                    ChunksReadiness::Ready(clock.now()),
                    false,
                ) {
                    let num_blocks_to_produce = if bp_ord < 3 { 2 } else { 1 };

                    for block_ord in 0..num_blocks_to_produce {
                        let parent_hash = ds.get_tip().0;

                        let prev_height = hash_to_block_info.get(&parent_hash).unwrap().0;
                        let prev_prev_height = if prev_height <= 1 {
                            0
                        } else {
                            let prev_prev_hash = hash_to_prev_hash.get(&parent_hash).unwrap();
                            hash_to_block_info.get(&prev_prev_hash).unwrap().0
                        };

                        let is_final =
                            target_height == prev_height + 1 && prev_height == prev_prev_height + 1;

                        let last_final_height = if is_final {
                            target_height - 2
                        } else {
                            hash_to_block_info.get(&parent_hash).unwrap().1
                        };

                        if target_height >= 2048 {
                            println!("Largest produced_height: {}", largest_produced_height);
                            for ds in &doomslugs {
                                println!(
                                    "  - tip: ({:?}), final_height: {}, timer height: {}",
                                    ds.get_tip(),
                                    ds.get_largest_final_height(),
                                    ds.get_timer_height()
                                );
                            }
                            assert!(false);
                            break 'outer;
                        }
                        let block_hash = block_hash(target_height, block_ord);
                        for whom in 0..8 {
                            let block_info = (
                                target_height,
                                whom,
                                last_final_height,
                                get_msg_delivery_time(clock.now(), gst, delta),
                                block_hash,
                            );
                            block_queue.push(block_info);
                        }

                        hash_to_block_info
                            .insert(block_hash, (target_height, last_final_height, block_hash));
                        hash_to_prev_hash.insert(block_hash, parent_hash);

                        assert!(!chain_lengths.contains_key(&block_hash));
                        let prev_length = *chain_lengths.get(&ds.get_tip().0).unwrap();
                        chain_lengths.insert(block_hash, prev_length + 1);

                        if is_final && target_height != 2 {
                            blocks_with_finality.push((
                                *hash_to_prev_hash.get(&parent_hash).unwrap(),
                                target_height - 2,
                            ));
                        }

                        if target_height > largest_produced_height {
                            largest_produced_height = target_height;
                        }
                        if target_height >= height_goal && is_final {
                            assert!(prev_length + 1 > 20); // make sure we actually built some chain
                            is_done = true;
                        }

                        // Accept our own block (only accept the last if are maliciously producing multiple,
                        // so that `ds.get_tip(...)` doesn't return the new block on the next iteration)
                        if block_ord + 1 == num_blocks_to_produce {
                            ds.set_tip(block_hash, target_height as BlockHeight, last_final_height);
                        }
                    }
                }
            }
        }

        // 5. Validate the timers
        assert_timers_in_sync(&doomslugs, clock.now(), delta);
    }

    // We successfully got to the `height_goal`. Check that all the blocks are building only on
    // doomslug final blocks
    for (block_hash, (block_height, _, _)) in &hash_to_block_info {
        let mut seen_hashes = HashSet::new();
        let mut block_hash = *block_hash;
        seen_hashes.insert(block_hash);

        loop {
            match hash_to_prev_hash.get(&block_hash) {
                None => break,
                Some(prev_block_hash) => {
                    block_hash = *prev_block_hash;
                    seen_hashes.insert(block_hash);
                }
            }
        }

        for (block_hash, height) in &blocks_with_finality {
            assert!(*height >= *block_height || seen_hashes.contains(block_hash));
        }
    }

    (clock.now().signed_duration_since(started), largest_produced_height)
}

#[test]
fn ultra_slow_test_fuzzy_doomslug_liveness_and_safety() {
    for (time_to_gst_millis, height_goal) in
        &[(0, 200), (1000, 200), (10000, 300), (100000, 400), (500000, 500)]
    {
        for delta in &[100, 300, 500, 1000, 2000, 4000] {
            println!("Staring set of tests. Time to GST: {}, delta: {}", time_to_gst_millis, delta);
            for _iter in 0..10 {
                let (took, height) = one_iter(
                    Duration::milliseconds(*time_to_gst_millis),
                    Duration::milliseconds(*delta),
                    *height_goal,
                );
                println!(
                    " --> Took {} (simulated) milliseconds and {} heights",
                    took.whole_milliseconds(),
                    height
                );
            }
        }
    }
}
