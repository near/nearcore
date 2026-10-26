use crate::setup::builder::TestLoopBuilder;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::spice::partial_data::SpiceDataIdentifier;
use near_primitives::types::{AccountId, EpochId, ShardId};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const EPOCH_LENGTH: u64 = 10;
const NUM_EPOCHS: u64 = 4;

/// The source shard's producers in the source block's epoch push a receipt proof to the
/// destination shard's producers in the next block's epoch, which differ from the source epoch's
/// on the last block of an epoch once chunk producers are shuffled between shards.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_receipt_proof_push_goes_to_the_next_blocks_destination_producers() {
    init_test_logger();
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(2)
        .epoch_length(EPOCH_LENGTH)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .track_all_shards()
        .build();

    let pushed: Arc<Mutex<HashMap<(CryptoHash, ShardId, ShardId), HashSet<AccountId>>>> =
        Default::default();
    for node in &env.node_datas {
        let pushed = pushed.clone();
        env.test_loop.data.get_mut(&node.peer_manager_sender.actor_handle()).register_observer(
            Box::new(move |request| {
                let NetworkRequests::SpicePartialData { partial_data, recipients } = request else {
                    return;
                };
                let SpiceDataIdentifier::ReceiptProof { block_hash, from_shard_id, to_shard_id } =
                    partial_data.id()
                else {
                    return;
                };
                pushed
                    .lock()
                    .entry((*block_hash, *from_shard_id, *to_shard_id))
                    .or_default()
                    .extend(recipients.iter().cloned());
            }),
        );
    }

    let observer = env.node_datas[0].account_id.clone();
    let target = env.node_for_account(&observer).head().height + NUM_EPOCHS * EPOCH_LENGTH;
    env.runner_for_account(&observer).run_until_certified(target);

    let node = env.node_for_account(&observer);
    let epoch_manager = node.client().epoch_manager.as_ref();
    let producers = |epoch_id: &EpochId, shard_id: ShardId| -> HashSet<AccountId> {
        epoch_manager
            .get_epoch_chunk_producers_for_shard(epoch_id, shard_id)
            .unwrap()
            .into_iter()
            .collect()
    };
    let mut pushed_across_a_producer_change = 0;
    for ((block_hash, from_shard, to_shard), recipients) in pushed.lock().iter() {
        let source_epoch_id = *node.client().chain.get_block_header(block_hash).unwrap().epoch_id();
        let next_epoch_id = epoch_manager.get_epoch_id_from_prev_block(block_hash).unwrap();
        let source_producers = producers(&source_epoch_id, *from_shard);
        let expected: HashSet<_> =
            producers(&next_epoch_id, *to_shard).difference(&source_producers).cloned().collect();
        assert_eq!(recipients, &expected, "receipt proof {block_hash} {from_shard}->{to_shard}");
        let in_source_epoch: HashSet<_> =
            producers(&source_epoch_id, *to_shard).difference(&source_producers).cloned().collect();
        if in_source_epoch != expected {
            pushed_across_a_producer_change += 1;
        }
    }
    assert!(
        pushed_across_a_producer_change > 0,
        "no receipt proof was pushed from the last block of an epoch whose destination producers changed"
    );
}
