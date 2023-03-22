use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock, RwLockWriteGuard};

use actix::System;
use near_chain::test_utils::ValidatorSchedule;
use rand::{thread_rng, Rng};

use crate::adapter::{BlockApproval, BlockResponse};
use crate::test_utils::{setup_mock_all_validators, ActorHandlesForTesting};
use near_actix_test_utils::run_actix;
use near_chain::Block;
use near_network::types::PeerInfo;
use near_network::types::{NetworkRequests, NetworkResponses, PeerManagerMessageRequest};
use near_o11y::testonly::init_integration_logger;
use near_o11y::WithSpanContextExt;
use near_primitives::block::{Approval, ApprovalInner};
use near_primitives::types::{AccountId, BlockHeight};

/// Rotates three independent sets of block producers producing blocks with a very short epoch length.
/// Occasionally when an endorsement comes, make all the endorsers send a skip message far-ish into
/// the future, and delay the distribution of the block produced this way.
/// Periodically verify finality is not violated.
/// This test is designed to reproduce finality bugs on the epoch boundaries.
#[test]
#[cfg_attr(not(feature = "expensive_tests"), ignore)]
fn test_consensus_with_epoch_switches() {
    init_integration_logger();

    const HEIGHT_GOAL: u64 = 120;

    run_actix(async move {
        let connectors: Arc<RwLock<Vec<ActorHandlesForTesting>>> = Arc::new(RwLock::new(vec![]));
        let connectors1 = connectors.clone();

        let validators: Vec<Vec<AccountId>> = [
            [
                "test1.1", "test1.2", "test1.3", "test1.4", "test1.5", "test1.6", "test1.7",
                "test1.8",
            ],
            [
                "test2.1", "test2.2", "test2.3", "test2.4", "test2.5", "test2.6", "test2.7",
                "test2.8",
            ],
            [
                "test3.1", "test3.2", "test3.3", "test3.4", "test3.5", "test3.6", "test3.7",
                "test3.8",
            ],
        ]
        .iter()
        .map(|l| l.iter().map(|account_id| account_id.parse().unwrap()).collect())
        .collect();
        let vs =
            ValidatorSchedule::new().num_shards(8).block_producers_per_epoch(validators.clone());
        let key_pairs = (0..24).map(|_| PeerInfo::random()).collect::<Vec<_>>();
        let archive = vec![true; vs.all_block_producers().count()];
        let epoch_sync_enabled = vec![false; vs.all_block_producers().count()];

        let block_to_prev_block = Arc::new(RwLock::new(HashMap::new()));
        let block_to_height = Arc::new(RwLock::new(HashMap::new()));

        let all_blocks = Arc::new(RwLock::new(BTreeMap::new()));
        let final_block_heights = Arc::new(RwLock::new(HashSet::new()));

        let largest_target_height = Arc::new(RwLock::new(vec![0u64; 24]));
        let skips_per_height = Arc::new(RwLock::new(vec![]));

        let largest_block_height = Arc::new(RwLock::new(0u64));
        let delayed_blocks = Arc::new(RwLock::new(vec![]));

        let (_, conn, _) = setup_mock_all_validators(
            vs,
            key_pairs.clone(),
            true,
            1000,
            false,
            false,
            4,
            true,
            archive,
            epoch_sync_enabled,
            false,
            Box::new(move |_, from_whom: AccountId, msg: &PeerManagerMessageRequest| {
                let mut all_blocks: RwLockWriteGuard<BTreeMap<BlockHeight, Block>> =
                    all_blocks.write().unwrap();
                let mut final_block_heights = final_block_heights.write().unwrap();
                let mut block_to_height = block_to_height.write().unwrap();
                let mut block_to_prev_block = block_to_prev_block.write().unwrap();
                let mut largest_target_height = largest_target_height.write().unwrap();
                let mut skips_per_height = skips_per_height.write().unwrap();
                let mut largest_block_height = largest_block_height.write().unwrap();

                let mut delayed_blocks = delayed_blocks.write().unwrap();

                match msg.as_network_requests_ref() {
                    NetworkRequests::Block { block } => {
                        if !all_blocks.contains_key(&block.header().height()) {
                            println!(
                                "BLOCK @{} EPOCH: {:?}, APPROVALS: {:?}",
                                block.header().height(),
                                block.header().epoch_id(),
                                block
                                    .header()
                                    .approvals()
                                    .iter()
                                    .map(|x| if x.is_some() { 1 } else { 0 })
                                    .collect::<Vec<_>>()
                            );
                        }
                        all_blocks.insert(block.header().height(), block.clone());
                        block_to_prev_block.insert(*block.hash(), *block.header().prev_hash());
                        block_to_height.insert(*block.hash(), block.header().height());

                        if *largest_block_height / 20 < block.header().height() / 20 {
                            // Periodically verify the finality
                            println!("VERIFYING FINALITY CONDITIONS");
                            for block in all_blocks.values() {
                                if let Some(prev_hash) = block_to_prev_block.get(&block.hash()) {
                                    if let Some(prev_height) = block_to_height.get(prev_hash) {
                                        let cur_height = block.header().height();
                                        for f in final_block_heights.iter() {
                                            if f < &cur_height && f > prev_height {
                                                assert!(
                                                    false,
                                                    "{} < {} < {}",
                                                    prev_height, f, cur_height
                                                );
                                            }
                                        }
                                    }
                                }
                            }

                            if *largest_block_height >= HEIGHT_GOAL {
                                System::current().stop();
                            }
                        }

                        if block.header().height() > *largest_block_height + 3 {
                            *largest_block_height = block.header().height();
                            if delayed_blocks.len() < 2 {
                                delayed_blocks.push(block.clone());
                                return (NetworkResponses::NoResponse.into(), false);
                            }
                        }
                        *largest_block_height =
                            std::cmp::max(block.header().height(), *largest_block_height);

                        let mut new_delayed_blocks = vec![];
                        for delayed_block in delayed_blocks.iter() {
                            if delayed_block.hash() == block.hash() {
                                return (NetworkResponses::NoResponse.into(), false);
                            }
                            if delayed_block.header().height() <= block.header().height() + 2 {
                                for target_ord in 0..24 {
                                    connectors1.write().unwrap()[target_ord].client_actor.do_send(
                                        BlockResponse {
                                            block: delayed_block.clone(),
                                            peer_id: key_pairs[0].clone().id,
                                            was_requested: true,
                                        }
                                        .with_span_context(),
                                    );
                                }
                            } else {
                                new_delayed_blocks.push(delayed_block.clone())
                            }
                        }
                        *delayed_blocks = new_delayed_blocks;

                        let mut heights = vec![];
                        let mut cur_hash = *block.hash();
                        while let Some(height) = block_to_height.get(&cur_hash) {
                            heights.push(height);
                            cur_hash = *block_to_prev_block.get(&cur_hash).unwrap();
                            if heights.len() > 10 {
                                break;
                            }
                        }
                        // Use Doomslug finality, since without duplicate blocks at the same height
                        // it also provides safety under 1/3 faults
                        let is_final = heights.len() > 1 && heights[1] + 1 == *heights[0];
                        println!(
                            "IS_FINAL: {} DELAYED: ({:?}) BLOCK: {} HISTORY: {:?}",
                            is_final,
                            delayed_blocks.iter().map(|x| x.header().height()).collect::<Vec<_>>(),
                            block.hash(),
                            heights,
                        );

                        if is_final {
                            final_block_heights.insert(*heights[1]);
                        }
                    }
                    NetworkRequests::Approval { approval_message } => {
                        // Identify who we are, and whom we are sending this message to
                        let mut epoch_id = 100;
                        let mut destination_ord = 100;
                        let mut my_ord = 100;

                        for i in 0..validators.len() {
                            for j in 0..validators[i].len() {
                                if validators[i][j] == approval_message.target {
                                    epoch_id = i;
                                    destination_ord = j;
                                }
                                if validators[i][j] == from_whom {
                                    my_ord = i * 8 + j;
                                }
                            }
                        }
                        assert_ne!(epoch_id, 100);
                        assert_ne!(my_ord, 100);

                        // For each height we define `skips_per_height`, and each block producer sends
                        // skips that far into the future from that source height.
                        let source_height = match approval_message.approval.inner {
                            ApprovalInner::Endorsement(_) => {
                                if largest_target_height[my_ord]
                                    >= approval_message.approval.target_height
                                    && my_ord % 8 >= 2
                                {
                                    // We already manually sent a skip conflicting with this endorsement
                                    // my_ord % 8 < 2 are two malicious actors in every epoch and they
                                    // continue sending endorsements
                                    return (NetworkResponses::NoResponse.into(), false);
                                }

                                approval_message.approval.target_height - 1
                            }
                            ApprovalInner::Skip(source_height) => source_height,
                        };

                        while source_height as usize >= skips_per_height.len() {
                            skips_per_height.push(if thread_rng().gen_bool(0.8) {
                                0
                            } else {
                                thread_rng().gen_range(2..9)
                            });
                        }
                        if skips_per_height[source_height as usize] > 0
                            && approval_message.approval.target_height - source_height == 1
                        {
                            let delta = skips_per_height[source_height as usize];
                            let approval = Approval {
                                target_height: approval_message.approval.target_height
                                    + delta as u64,
                                inner: ApprovalInner::Skip(source_height),
                                ..approval_message.approval.clone()
                            };
                            largest_target_height[my_ord] = std::cmp::max(
                                largest_target_height[my_ord],
                                approval.target_height as u64,
                            );
                            connectors1.write().unwrap()
                                [epoch_id * 8 + (destination_ord + delta) % 8]
                                .client_actor
                                .do_send(
                                    BlockApproval(approval, key_pairs[my_ord].id.clone())
                                        .with_span_context(),
                                );
                            // Do not send the endorsement for couple block producers in each epoch
                            // This is needed because otherwise the block with enough endorsements
                            // sometimes comes faster than the sufficient number of skips is created,
                            // (because the block producer themselves doesn't send the endorsement
                            // over the network, they have one more approval ready to produce their
                            // block than the block producer that will be at the later height). If
                            // such a block is indeed produced faster than all the skips are created,
                            // the paritcipants who haven't sent their endorsements to be converted
                            // to skips change their head.
                            if my_ord % 8 < 2 {
                                return (NetworkResponses::NoResponse.into(), false);
                            }
                        }
                    }
                    _ => {}
                };
                (NetworkResponses::NoResponse.into(), true)
            }),
        );
        *connectors.write().unwrap() = conn;

        // We only check the terminating condition once every 20 heights, thus extra 80 to
        // account for possibly going beyond the HEIGHT_GOAL.
        near_network::test_utils::wait_or_panic(3000 * (80 + HEIGHT_GOAL));
    });
}
