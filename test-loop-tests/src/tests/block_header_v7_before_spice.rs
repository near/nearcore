use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::HandlerResult;
use near_async::time::Duration;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::block_header::{BlockHeader, BlockHeaderInnerRestV7, BlockHeaderV7};
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, BlockHeight, EpochId};
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use std::sync::Arc;

fn convert_v6_header_to_v7(header: &BlockHeader) -> BlockHeader {
    let BlockHeader::BlockHeaderV6(header_v6) = header else {
        panic!("expected BlockHeaderV6, got {header:?}");
    };
    let rest = header_v6.inner_rest.clone();
    let inner_rest = BlockHeaderInnerRestV7 {
        block_body_hash: rest.block_body_hash,
        prev_chunk_outgoing_receipts_root: rest.prev_chunk_outgoing_receipts_root,
        chunk_headers_root: rest.chunk_headers_root,
        chunk_tx_root: rest.chunk_tx_root,
        random_value: rest.random_value,
        prev_validator_proposals: rest.prev_validator_proposals,
        chunk_mask: rest.chunk_mask,
        next_gas_price: rest.next_gas_price,
        total_supply: rest.total_supply,
        last_final_block: rest.last_final_block,
        last_ds_final_block: rest.last_ds_final_block,
        block_ordinal: rest.block_ordinal,
        prev_height: rest.prev_height,
        epoch_sync_data_hash: rest.epoch_sync_data_hash,
        approvals: rest.approvals,
        latest_protocol_version: rest.latest_protocol_version,
        chunk_endorsements: rest.chunk_endorsements,
        shard_split: rest.shard_split,
        prev_last_certified_block_epoch_id: EpochId::default(),
        spice_chunk_endorsement_stats: vec![],
    };
    BlockHeader::BlockHeaderV7(BlockHeaderV7 {
        prev_hash: header_v6.prev_hash,
        inner_lite: header_v6.inner_lite.clone(),
        inner_rest,
        signature: header_v6.signature.clone(),
        hash: header_v6.hash,
    })
}

struct V7BlockBroadcast {
    v7_height: BlockHeight,
    v7_producer: AccountId,
}

/// Builds a 4-validator chain at `protocol_version`. The producer of `v7_height` keeps its V6
/// block and broadcasts a copy with a `BlockHeaderV7` header. That producer builds on its V6
/// block, so `v7_height` is picked such that another validator produces the next height.
fn setup_v7_block_broadcast(protocol_version: ProtocolVersion) -> (TestLoopEnv, V7BlockBroadcast) {
    let mut env =
        TestLoopBuilder::new().validators(4, 0).protocol_version(protocol_version).build();
    let head = env.node(0).head();
    let epoch_manager = env.node(0).client().epoch_manager.clone();
    let block_producer = |height| epoch_manager.get_block_producer(&head.epoch_id, height).unwrap();
    let v7_height = (head.height + 3..)
        .find(|&height| block_producer(height) != block_producer(height + 1))
        .unwrap();
    for node_data in &env.node_datas {
        let signer = create_test_signer(node_data.account_id.as_str());
        let peer_actor = env.test_loop.data.get_mut(&node_data.peer_manager_sender.actor_handle());
        peer_actor.register_override_handler(Box::new(move |request| match request {
            NetworkRequests::Block { block }
                if block.header().height() == v7_height
                    && matches!(block.header(), BlockHeader::BlockHeaderV6(_)) =>
            {
                let mut v7_block = (*block).clone();
                *v7_block.mut_header() = convert_v6_header_to_v7(block.header());
                v7_block.mut_header().resign(&signer);
                HandlerResult::Unhandled(NetworkRequests::Block { block: Arc::new(v7_block) })
            }
            other => HandlerResult::Unhandled(other),
        }));
    }
    let v7_producer = block_producer(v7_height);
    (env, V7BlockBroadcast { v7_height, v7_producer })
}

#[test]
fn block_header_v7_accepted_before_reject_feature() {
    init_test_logger();
    let protocol_version = ProtocolFeature::RejectBlockHeaderV7BeforeSpice.protocol_version() - 1;
    let (mut env, broadcast) = setup_v7_block_broadcast(protocol_version);
    let v7_height = broadcast.v7_height;

    env.test_loop.run_for(Duration::seconds(15));

    for node_index in 0..env.node_datas.len() {
        let node = env.node(node_index);
        let chain = &node.client().chain;
        let canonical_hash_at_v7_height =
            chain.chain_store.get_block_hash_by_height(v7_height).unwrap();
        let canonical_header_at_v7_height =
            chain.get_block_header(&canonical_hash_at_v7_height).unwrap();
        assert!(matches!(*canonical_header_at_v7_height, BlockHeader::BlockHeaderV7(_)));
        assert!(node.final_head().height > v7_height);
    }
}

#[test]
fn block_header_v7_rejected_with_reject_feature() {
    init_test_logger();
    let protocol_version = ProtocolFeature::RejectBlockHeaderV7BeforeSpice.protocol_version();
    let (mut env, broadcast) = setup_v7_block_broadcast(protocol_version);
    let v7_height = broadcast.v7_height;

    env.test_loop.run_for(Duration::seconds(15));

    for node_data in &env.node_datas {
        let node = env.node_for_account(&node_data.account_id);
        let chain = &node.client().chain;
        let hashes_at_v7_height = chain.chain_store.get_all_block_hashes_by_height(v7_height);
        for block_hash in hashes_at_v7_height.values().flatten() {
            let header = chain.get_block_header(block_hash).unwrap();
            assert!(!matches!(*header, BlockHeader::BlockHeaderV7(_)));
        }
        let final_height = node.final_head().height;
        if node_data.account_id == broadcast.v7_producer {
            assert!(final_height < v7_height);
        } else {
            assert!(final_height > v7_height);
        }
    }
}
