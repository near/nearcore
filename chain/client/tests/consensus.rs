use actix::{Addr, System};
use near_chain::Block;
use near_client::test_utils::setup_mock_all_validators;
use near_client::{ClientActor, ViewClientActor};
use near_network::{NetworkClientMessages, NetworkRequests, NetworkResponses, PeerInfo};
use near_primitives::block::{Approval, ApprovalInner};
use near_primitives::test_utils::init_integration_logger;
use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockWriteGuard};

#[test]
fn test_fuzzy_consensus_with_epoch_switches() {
    init_integration_logger();

    System::run(move || {
        let connectors: Arc<RwLock<Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>>> =
            Arc::new(RwLock::new(vec![]));
        let connectors1 = connectors.clone();

        let validators = vec![
            vec![
                "test1.1", "test1.2", "test1.3", "test1.4", "test1.5", "test1.6", "test1.7",
                "test1.8",
            ],
            vec![
                "test2.1", "test2.2", "test2.3", "test2.4", "test2.5", "test2.6", "test2.7",
                "test2.8",
            ],
            vec![
                "test3.1", "test3.2", "test3.3", "test3.4", "test3.5", "test3.6", "test3.7",
                "test3.8",
            ],
        ];
        let key_pairs = (0..24).map(|_| PeerInfo::random()).collect::<Vec<_>>();

        let block_to_prev_block = Arc::new(RwLock::new(HashMap::new()));
        let block_to_height = Arc::new(RwLock::new(HashMap::new()));

        let all_blocks = Arc::new(RwLock::new(vec![]));

        let largest_target_height = Arc::new(RwLock::new(vec![0u64; 24]));
        let largest_block_ord_sent = Arc::new(RwLock::new(vec![0usize; 24]));
        let skips_per_height = Arc::new(RwLock::new(vec![]));
        let block_delays_per_height = Arc::new(RwLock::new(vec![]));

        let largest_block_height_ever_sent = Arc::new(RwLock::new(0u64));

        let (_, conn) = setup_mock_all_validators(
            validators.clone(),
            key_pairs.clone(),
            1,
            true,
            1000,
            false,
            false,
            24,
            true,
            vec![false; validators.iter().map(|x| x.len()).sum()],
            Arc::new(RwLock::new(Box::new(move |from_whom: String, msg: &NetworkRequests| {
                let mut all_blocks: RwLockWriteGuard<Vec<Block>> = all_blocks.write().unwrap();
                let mut block_to_height = block_to_height.write().unwrap();
                let mut block_to_prev_block = block_to_prev_block.write().unwrap();
                let mut largest_target_height = largest_target_height.write().unwrap();
                let mut largest_block_ord_sent = largest_block_ord_sent.write().unwrap();
                let mut skips_per_height = skips_per_height.write().unwrap();
                let mut block_delays_per_height = block_delays_per_height.write().unwrap();
                let mut largest_block_height_ever_sent =
                    largest_block_height_ever_sent.write().unwrap();

                while block_delays_per_height.last().cloned().unwrap_or_else(|| 0) > 1 {
                    let el = block_delays_per_height.last().cloned().unwrap() - 1;
                    block_delays_per_height.push(el);
                }

                match msg {
                    NetworkRequests::Block { block } => {
                        if all_blocks.len() > 0
                            && all_blocks.last().as_ref().unwrap().header.inner_lite.height
                                > block.header.inner_lite.height
                        {
                            // This is one of the existing blocks being requested
                            return (NetworkResponses::NoResponse, true);
                        }
                        all_blocks.push(block.clone());
                        block_to_prev_block.insert(block.hash(), block.header.prev_hash);
                        block_to_height.insert(block.hash(), block.header.inner_lite.height);

                        let mut heights = vec![];
                        let mut cur_hash = block.hash();
                        while let Some(height) = block_to_height.get(&cur_hash) {
                            heights.push(height);
                            cur_hash = block_to_prev_block.get(&cur_hash).unwrap().clone();
                        }
                        println!("MOO1 {} => {:?}", block.hash(), heights);

                        while block_delays_per_height.len()
                            <= block.header.inner_lite.height as usize
                        {
                            block_delays_per_height.push(if thread_rng().gen_bool(0.5) {
                                1
                            } else {
                                thread_rng().gen_range(1, 6)
                            });
                        }

                        if block_delays_per_height[block.header.inner_lite.height as usize] == 1 {
                            return (NetworkResponses::NoResponse, true);
                        } else {
                            return (NetworkResponses::NoResponse, false);
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

                        // If it's been a while since we distributed blocks, send some blocks
                        // Half the time send just one extra block to everybody.
                        // Quarter of the time send the same prefix of blocks
                        // Quarter of the time send different prefixes
                        while block_delays_per_height.len()
                            <= *largest_block_height_ever_sent as usize
                        {
                            block_delays_per_height.push(if thread_rng().gen_bool(0.5) {
                                1
                            } else {
                                thread_rng().gen_range(1, 6)
                            });
                        }

                        if approval_message.approval.target_height
                            > *largest_block_height_ever_sent
                                + block_delays_per_height[*largest_block_height_ever_sent as usize]
                            && all_blocks.len() > 0
                        {
                            let mode = if thread_rng().gen_bool(0.5) {
                                0
                            } else if thread_rng().gen_bool(0.5) {
                                1
                            } else {
                                2
                            };
                            for target_ord in 0..key_pairs.len() {
                                let new_max = if mode == 0
                                    && largest_block_ord_sent[target_ord] + 1 < all_blocks.len()
                                {
                                    largest_block_ord_sent[target_ord] + 1
                                } else if mode == 1 && target_ord > 0 {
                                    largest_block_ord_sent[target_ord - 1]
                                } else {
                                    thread_rng().gen_range(
                                        largest_block_ord_sent[target_ord],
                                        all_blocks.len(),
                                    )
                                };
                                for block_ord in largest_block_ord_sent[target_ord] + 1..=new_max {
                                    connectors1.write().unwrap()[target_ord].0.do_send(
                                        NetworkClientMessages::Block(
                                            all_blocks[block_ord].clone(),
                                            key_pairs[my_ord].clone().id,
                                            false,
                                        ),
                                    );
                                }
                                largest_block_ord_sent[target_ord] = new_max;
                                *largest_block_height_ever_sent = std::cmp::max(
                                    *largest_block_height_ever_sent,
                                    all_blocks[new_max].header.inner_lite.height,
                                );
                            }
                        }

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
                                    return (NetworkResponses::NoResponse, false);
                                }

                                approval_message.approval.target_height - 1
                            }
                            ApprovalInner::Skip(source_height) => source_height,
                        };

                        while source_height as usize >= skips_per_height.len() {
                            skips_per_height.push(if thread_rng().gen_bool(0.8) {
                                0
                            } else {
                                thread_rng().gen_range(1, 7)
                            });
                        }
                        for j in 1usize..skips_per_height[source_height as usize] {
                            let approval = Approval {
                                target_height: approval_message.approval.target_height + j as u64,
                                inner: ApprovalInner::Skip(source_height),
                                ..approval_message.approval.clone()
                            };
                            largest_target_height[my_ord] = std::cmp::max(
                                largest_target_height[my_ord],
                                approval.target_height + j as u64,
                            );
                            connectors1.write().unwrap()[epoch_id * 8 + (destination_ord + j) % 8]
                                .0
                                .do_send(NetworkClientMessages::BlockApproval(
                                    approval,
                                    key_pairs[my_ord].id.clone(),
                                ))
                        }
                    }
                    _ => {}
                };
                (NetworkResponses::NoResponse, true)
            }))),
        );
        *connectors.write().unwrap() = conn;

        near_network::test_utils::wait_or_panic(400000);
    })
    .unwrap();
}
