// No merkle root covers `ShardProof::from_shard_id`, and `compute_state_response_header` uses it as
// a chunk index. `set_state_header` binds it by checking the label against the merkle path index,
// and by requiring that index to name a chunk the block included.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use near_async::time::Duration;
use near_chain::Chain;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::merklize;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_sync::{RootProof, ShardStateSyncResponseHeader};
use near_primitives::types::ShardId;
use std::collections::HashMap;
use std::sync::Arc;

fn await_sync_hash(env: &mut TestLoopEnv) -> CryptoHash {
    // Past the genesis epoch, so the layout lookup the check performs uses a real epoch id.
    env.node_runner(0).run_until_new_epoch();
    env.node_runner(0).run_until(
        |node| node.client().chain.get_sync_hash(&node.head().last_block_hash).unwrap().is_some(),
        Duration::seconds(20),
    );
    let node = env.node(0);
    node.client().chain.get_sync_hash(&node.head().last_block_hash).unwrap().unwrap()
}

/// Only the labels move. The receipts and the paths stay as they were, and the set of labels is
/// unchanged, so the uniqueness check and every merkle check still pass.
fn swap_first_two_from_shard_ids(header: &mut ShardStateSyncResponseHeader) {
    let ShardStateSyncResponseHeader::V2(header) = header else {
        panic!("expected a V2 state sync header");
    };
    for response in &mut header.incoming_receipts_proofs {
        let proofs = Arc::make_mut(&mut response.1);
        assert!(proofs.len() >= 2, "need two receipt proofs per block to swap labels");
        let first_label = proofs[0].1.from_shard_id;
        proofs[0].1.from_shard_id = proofs[1].1.from_shard_id;
        proofs[1].1.from_shard_id = first_label;
        assert_ne!(proofs[0].1.from_shard_id, first_label, "both proofs carried one label");
    }
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_set_state_header_rejects_swapped_from_shard_id() {
    init_test_logger();
    // The sync hash is stored only after it becomes final. With the default epoch length of 5 that
    // happens after the head moves to the next epoch, so `await_sync_hash` never finds one.
    let mut env = TestLoopBuilder::new()
        .validators(2, 0)
        .num_shards(2)
        .epoch_length(10)
        .track_all_shards()
        .build();

    let sync_hash = await_sync_hash(&mut env);
    let shard_id = ShardId::new(0);
    let genuine_header = env
        .node(0)
        .client()
        .chain
        .state_sync_adapter
        .compute_state_response_header(shard_id, sync_hash)
        .unwrap();

    let victim = &env.node(1).client().chain.state_sync_adapter;
    victim.set_state_header(shard_id, sync_hash, genuine_header.clone()).unwrap();

    let mut tampered_header = genuine_header;
    swap_first_two_from_shard_ids(&mut tampered_header);

    let rejection = victim.set_state_header(shard_id, sync_hash, tampered_header).unwrap_err();
    assert!(
        rejection.to_string().contains("invalid proofs"),
        "unexpected rejection: {rejection:?}"
    );
}

/// Points one proof at a shard that produced no chunk in its block. Such a shard keeps the previous
/// chunk header, so its leaf in the receipts-root tree repeats an older root, and the proof that
/// verified against that root at its own height still verifies here.
fn point_proof_at_shard_without_a_chunk(
    chain: &Chain,
    header: &mut ShardStateSyncResponseHeader,
    shard_layout: &ShardLayout,
) -> ShardId {
    let ShardStateSyncResponseHeader::V2(header) = header else {
        panic!("expected a V2 state sync header");
    };
    // Responses run newest first, so the block before `responses[i]` is `responses[i + 1]`.
    for i in 0..header.incoming_receipts_proofs.len() - 1 {
        let block = chain.get_block(&header.incoming_receipts_proofs[i].0).unwrap();
        let Some(stale_index) = block.chunks().iter().position(|chunk| !chunk.is_new_chunk())
        else {
            continue;
        };
        let stale_shard_id = shard_layout.get_shard_id(stale_index).unwrap();
        let earlier_proof_from_stale_shard = header.incoming_receipts_proofs[i + 1]
            .1
            .iter()
            .find(|proof| proof.1.from_shard_id == stale_shard_id)
            .expect("previous block has no proof from the shard whose chunk is stale")
            .clone();

        let (_, receipts_root_paths) = merklize(
            &block
                .chunks()
                .iter_raw()
                .map(|chunk| *chunk.prev_outgoing_receipts_root())
                .collect::<Vec<CryptoHash>>(),
        );
        let stale_root =
            *block.chunks().iter_raw().nth(stale_index).unwrap().prev_outgoing_receipts_root();

        let proofs = Arc::make_mut(&mut header.incoming_receipts_proofs[i].1);
        assert_ne!(proofs[0].1.from_shard_id, stale_shard_id);
        proofs[0] = earlier_proof_from_stale_shard;
        header.root_proofs[i][0] = RootProof(stale_root, receipts_root_paths[stale_index].clone());
        return stale_shard_id;
    }
    panic!("no block in the receipt range has a shard without a new chunk");
}

#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_set_state_header_rejects_from_shard_id_without_a_chunk() {
    init_test_logger();
    let mut env = TestLoopBuilder::new()
        .validators(2, 0)
        .num_shards(2)
        .epoch_length(10)
        .track_all_shards()
        .delay_warmup()
        .build()
        .drop(DropCondition::ChunksProducedByHeight(HashMap::from([(
            ShardId::new(1),
            vec![true, true, true, false, false],
        )])))
        .warmup();

    let sync_hash = await_sync_hash(&mut env);
    let shard_id = ShardId::new(0);
    let node = env.node(0);
    let epoch_id = *node.client().chain.get_block_header(&sync_hash).unwrap().epoch_id();
    let shard_layout = node.client().epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let genuine_header = node
        .client()
        .chain
        .state_sync_adapter
        .compute_state_response_header(shard_id, sync_hash)
        .unwrap();

    let mut tampered_header = genuine_header.clone();
    let stale_shard_id = point_proof_at_shard_without_a_chunk(
        &node.client().chain,
        &mut tampered_header,
        &shard_layout,
    );

    let victim = &env.node(1).client().chain.state_sync_adapter;
    victim.set_state_header(shard_id, sync_hash, genuine_header).unwrap();

    let rejection = victim.set_state_header(shard_id, sync_hash, tampered_header).unwrap_err();
    assert!(
        rejection.to_string().contains("invalid proofs"),
        "planted a proof from shard {stale_shard_id}, unexpected rejection: {rejection:?}"
    );
}
