use crate::setup::builder::TestLoopBuilder;
use crate::setup::peer_manager_actor::HandlerResult;
use near_async::messaging::CanSend as _;
use near_async::time::Duration;
use near_chain::spice::core::get_last_certified_block_header;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::recv_permit::RecvMessagePermit;
use near_network::types::{NetworkRequests, NetworkResponses};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, AccountInfo, Balance, BlockHeight, ShardId};
use parking_lot::Mutex;
use std::sync::Arc;

/// Lowest height of the chunk whose endorsements reach the block producer late.
const MIN_HOLD_HEIGHT: BlockHeight = 10;
/// Chunks a late endorser endorses before releasing the held endorsement.
const HOLD_FOR_CHUNKS: BlockHeight = 10;

/// With 4 equal validators a chunk certifies at 3 endorsements. Two validators' endorsements of the
/// chunk at the hold height reach the only block producer late, so on chain it stays at 2. The
/// child's chunk producer is another validator, which sees all 4 and certifies the parent locally,
/// so it executes the child on time. Its validators endorse it, which certifies the child at the
/// block producer before its parent.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_late_parent_endorsement_does_not_halt_block_production() {
    init_test_logger();
    let accounts: Vec<AccountId> =
        (0..4).map(|i| format!("validator{i}").parse().unwrap()).collect();
    let validators: Vec<AccountInfo> = accounts
        .iter()
        .map(|account_id| AccountInfo {
            public_key: create_test_signer(account_id.as_str()).public_key(),
            account_id: account_id.clone(),
            amount: Balance::from_near(100),
        })
        .collect();
    let validators_spec = ValidatorsSpec::raw(validators, 1, 2, 4);
    let mut env = TestLoopBuilder::new()
        .shard_layout(ShardLayout::single_shard())
        .validators_spec(validators_spec)
        .epoch_length(100)
        .build();

    let epoch_manager = env.node(0).client().epoch_manager.clone();
    let epoch_id = env.node(0).head().epoch_id;
    let producers = epoch_manager.get_epoch_block_producers_ordered(&epoch_id).unwrap();
    assert_eq!(producers.len(), 1);
    let producer = producers[0].account_id().clone();
    let producer_sender = env
        .node_datas
        .iter()
        .find(|node| node.account_id == producer)
        .unwrap()
        .spice_core_writer_sender
        .clone();

    let shard_id = ShardId::new(0);
    let chunk_producer = |height_created| {
        let key = ChunkProductionKey { shard_id, epoch_id, height_created };
        epoch_manager.get_chunk_producer_info(&key).unwrap().take_account_id()
    };
    let hold_height =
        (MIN_HOLD_HEIGHT..).find(|&height| chunk_producer(height + 1) != producer).unwrap();
    let child_chunk_producer = chunk_producer(hold_height + 1);
    let late_endorsers: Vec<usize> = env
        .node_datas
        .iter()
        .enumerate()
        .filter(|(_, node)| node.account_id != producer && node.account_id != child_chunk_producer)
        .map(|(index, _)| index)
        .collect();
    assert_eq!(late_endorsers.len(), 2);
    let released = Arc::new(Mutex::new(0));
    for index in late_endorsers {
        let epoch_manager = epoch_manager.clone();
        let producer = producer.clone();
        let producer_sender = producer_sender.clone();
        let released = released.clone();
        let held: Mutex<Vec<SpiceChunkEndorsement>> = Mutex::new(Vec::new());
        env.node_datas[index].register_override_handler(&mut env.test_loop.data, move |request| {
            let NetworkRequests::SpiceChunkEndorsement(target, endorsement) = request else {
                return HandlerResult::Unhandled(request);
            };
            let height = epoch_manager.get_block_info(endorsement.block_hash()).unwrap().height();
            let mut held = held.lock();
            if height >= hold_height + HOLD_FOR_CHUNKS && !held.is_empty() {
                for endorsement in held.drain(..) {
                    producer_sender
                        .send(SpiceChunkEndorsementMessage(endorsement, RecvMessagePermit::none()));
                }
                *released.lock() += 1;
            }
            if height == hold_height && target == producer {
                held.push(endorsement);
                return HandlerResult::Handled(NetworkResponses::NoResponse);
            }
            HandlerResult::Unhandled(NetworkRequests::SpiceChunkEndorsement(target, endorsement))
        });
    }

    let target_height = hold_height + 3 * HOLD_FOR_CHUNKS;
    env.node_runner(0).run_until(|node| node.head().height >= target_height, Duration::seconds(60));
    assert_eq!(*released.lock(), 2, "held endorsements were not released");

    let node = env.node_for_account(&producer);
    let client = node.client();
    let chain_store = &client.chain.chain_store;
    let head = node.head();
    // Every height has a block: block production never failed while the parent waited.
    let mut header = chain_store.get_block_header(&head.last_block_hash).unwrap();
    while !header.is_genesis() {
        let prev = chain_store.get_block_header(header.prev_hash()).unwrap();
        assert_eq!(prev.height() + 1, header.height(), "skipped height after {}", prev.height());
        header = prev;
    }
    let last_certified =
        get_last_certified_block_header(chain_store, &head.last_block_hash).unwrap();
    assert!(
        head.height <= last_certified.height() + 3,
        "certification lags: head {} last certified {}",
        head.height,
        last_certified.height()
    );
}
