use crate::setup::builder::TestLoopBuilder;
use crate::setup::network_rules::Action;
use near_crypto::{KeyType, PublicKey};
use near_network::types::{NetworkRequests, ReasonForBan};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::hash;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{Balance, validator_stake::ValidatorStake};
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
enum InvalidBlockMode {
    /// Header is invalid
    InvalidHeader,
    /// Block is ill-formed (roots check fail)
    IllFormed,
    /// Block is invalid for other reasons
    InvalidBlock,
}

fn ban_peer_for_invalid_block_common(mode: InvalidBlockMode) {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(4, 0).epoch_length(100).build();
    let epoch_manager = env.node(0).client().epoch_manager.clone();

    let ban_counter: Arc<RwLock<usize>> = Arc::new(RwLock::new(0));
    let bad_block_send_height = 8;
    // Every node rewrites the block at `bad_block_send_height` on its way out, and swallows
    // the bans the receivers answer with, counting them.
    for index in 0..env.node_datas.len() {
        let ban_counter = ban_counter.clone();
        let epoch_manager = epoch_manager.clone();
        let mode = mode.clone();

        env.network_rule(
            index,
            |request| {
                matches!(request, NetworkRequests::Block { .. } | NetworkRequests::BanPeer { .. })
            },
            Action::Replace(Box::new(move |request| {
                let mut ban_counter = ban_counter.write();
                match request {
                    NetworkRequests::Block { mut block } => {
                        let epoch_id = block.header().epoch_id();
                        let height = block.header().height();
                        if height == bad_block_send_height {
                            let (bp, _) = epoch_manager
                                .get_block_producer_info(&epoch_id, height)
                                .unwrap()
                                .account_and_stake();
                            let validator_signer = create_test_signer(bp.as_str());
                            let mut_block = Arc::make_mut(&mut block);
                            match mode {
                                InvalidBlockMode::InvalidHeader => {
                                    // produce an invalid block with invalid header.
                                    mut_block.mut_header().set_chunk_mask(vec![]);
                                    mut_block.mut_header().resign(&validator_signer);
                                }
                                InvalidBlockMode::IllFormed => {
                                    // produce an ill-formed block
                                    mut_block.mut_header().set_chunk_headers_root(hash(&[1]));
                                    mut_block.mut_header().resign(&validator_signer);
                                }
                                InvalidBlockMode::InvalidBlock => {
                                    // produce an invalid block whose invalidity cannot be verified by just
                                    // having its header.
                                    let proposals = vec![ValidatorStake::new(
                                        bp,
                                        PublicKey::empty(KeyType::ED25519),
                                        Balance::ZERO,
                                    )];

                                    mut_block.mut_header().set_prev_validator_proposals(proposals);
                                    mut_block.mut_header().resign(&validator_signer);
                                }
                            }
                        }
                        vec![NetworkRequests::Block { block }]
                    }
                    NetworkRequests::BanPeer { ref peer_id, ref ban_reason } => match mode {
                        InvalidBlockMode::InvalidHeader | InvalidBlockMode::IllFormed => {
                            assert_eq!(ban_reason, &ReasonForBan::BadBlockHeader);
                            *ban_counter += 1;
                            if *ban_counter > 3 {
                                panic!("more bans than expected");
                            }
                            Vec::new()
                        }
                        InvalidBlockMode::InvalidBlock => {
                            panic!("banning peer {:?} unexpectedly for {:?}", peer_id, ban_reason);
                        }
                    },
                    _ => vec![request],
                }
            })),
        );
    }

    env.node_runner(0).run_until_head_height(25);

    let ban_counter = *ban_counter.read();
    match mode {
        InvalidBlockMode::InvalidHeader | InvalidBlockMode::IllFormed => {
            assert_eq!(ban_counter, 3);
        }
        InvalidBlockMode::InvalidBlock => {
            assert_eq!(ban_counter, 0);
        }
    }
}

/// If a peer sends a block whose header is valid and passes basic validation, the peer is not banned.
#[test]
fn test_not_ban_peer_for_invalid_block() {
    ban_peer_for_invalid_block_common(InvalidBlockMode::InvalidBlock);
}

/// If a peer sends a block whose header is invalid, we should ban them and do not forward the block
#[test]
fn test_ban_peer_for_invalid_block_header() {
    ban_peer_for_invalid_block_common(InvalidBlockMode::InvalidHeader);
}

/// If a peer sends a block that is ill-formed, we should ban them and do not forward the block
#[test]
fn test_ban_peer_for_ill_formed_block() {
    ban_peer_for_invalid_block_common(InvalidBlockMode::IllFormed);
}

#[test]
fn test_produce_block_with_approvals_arrived_early() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(4, 0).num_shards(4).epoch_length(100).build();
    let epoch_manager = env.node(0).client().epoch_manager.clone();

    let approval_counter = Arc::new(AtomicUsize::new(0));
    let block_withholding_height = 10;
    let epoch_id = env.node(0).head().epoch_id;

    let (block_producer_for_next_height, _) = epoch_manager
        .get_block_producer_info(&epoch_id, block_withholding_height + 1)
        .unwrap()
        .account_and_stake();
    let (block_producer_for_height, _) = epoch_manager
        .get_block_producer_info(&epoch_id, block_withholding_height)
        .unwrap()
        .account_and_stake();
    // With the same block producer - block withholding wouldn't test much.
    assert_ne!(block_producer_for_height, block_producer_for_next_height);

    // The next producer gets the block only once the other three have approved its height, so
    // their approvals arrive before the block they are for.
    let network = env.shared_state.network_shared_state.clone();
    network.withhold_block_delivery(
        &block_producer_for_next_height,
        block_withholding_height..block_withholding_height + 1,
    );
    for index in 0..env.node_datas.len() {
        let network = network.clone();
        let approval_counter = approval_counter.clone();
        let next_producer = block_producer_for_next_height.clone();
        let is_approval_for_next_height = {
            let next_producer = next_producer.clone();
            move |request: &NetworkRequests| {
                matches!(request, NetworkRequests::Approval { approval_message }
                    if approval_message.target == next_producer
                        && approval_message.approval.target_height == block_withholding_height + 1)
            }
        };
        env.network_rule(
            index,
            is_approval_for_next_height,
            Action::Observe(Box::new(move |_| {
                if approval_counter.fetch_add(1, Ordering::SeqCst) + 1 == 3 {
                    let delivered = network.deliver_withheld_blocks(&next_producer);
                    assert_eq!(delivered, 1, "the block was not withheld before the approvals");
                }
            })),
        );
    }

    env.node_runner(0).run_until_final_head_height(block_withholding_height + 1);

    // Block after delayed one should still be produced though approvals for it arrived before the
    // block.
    assert!(env.node(0).client().chain.get_block_by_height(block_withholding_height + 1).is_ok());
}
