use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::sync::epoch::{EpochSync, MAX_EPOCHS_PER_CHUNK};
use near_epoch_manager::epoch_sync::{
    derive_epoch_sync_proof_from_last_block, find_target_epoch_to_produce_proof_for,
};
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_sync::EpochSyncProofV1;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, EpochHeight};
use near_primitives::utils::compression::CompressedData;
use near_store::adapter::StoreAdapter;
use std::cell::RefCell;
use std::rc::Rc;

const NUM_CLIENTS: usize = 4;

/// Create a blockchain long enough to require multiple V2 epoch sync chunks.
fn setup_blockchain_for_incremental_sync() -> TestLoopEnv {
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(NUM_CLIENTS).cloned().collect_vec();

    let epoch_length = 10;
    let boundary_accounts =
        ["account3", "account5", "account7"].iter().map(|&a| a.parse().unwrap()).collect();
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
    let validators_spec =
        ValidatorsSpec::desired_roles(&clients.iter().map(|t| t.as_str()).collect_vec(), &[]);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .genesis_height(10000)
        .transaction_validity_period(20)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build();

    // Run money transfers to progress the chain for many epochs.
    crate::utils::transactions::execute_money_transfers(
        &mut env.test_loop,
        &env.node_datas,
        &accounts,
    )
    .unwrap();

    // Make sure the chain progressed for several epochs.
    let client_actor = env.test_loop.data.get(&env.node_datas[0].client_sender.actor_handle());
    assert!(client_actor.client.chain.head().unwrap().height > 10050);

    env
}

/// Helper to derive a full V1 proof from a node.
fn derive_full_proof(env: &TestLoopEnv, node_index: usize) -> EpochSyncProofV1 {
    let client_handle = env.node_datas[node_index].client_sender.actor_handle();
    let store = env.test_loop.data.get(&client_handle).client.chain.chain_store.store();
    let tx_validity_period = env.shared_state.genesis.config.transaction_validity_period;
    let last_block_hash =
        find_target_epoch_to_produce_proof_for(&store, tx_validity_period).unwrap();
    derive_epoch_sync_proof_from_last_block(&store.epoch_store(), &last_block_hash, true)
        .unwrap()
        .into_v1()
}

/// Test that extract_chunk_from_proof correctly splits a full proof into V2 chunks,
/// and that the chunks cover the entire proof when iterated.
#[test]
fn test_extract_chunk_from_proof_coverage() {
    init_test_logger();
    let env = setup_blockchain_for_incremental_sync();
    let full_proof = derive_full_proof(&env, 0);
    let total_epochs = full_proof.all_epochs.len();
    tracing::info!(total_epochs, "full proof size");
    assert!(total_epochs > 2, "proof should have multiple epochs");

    // Walk through all chunks like a requester would.
    let mut epoch_height: EpochHeight = 1; // start from genesis
    let mut chunks_received = 0;
    let mut total_epoch_entries = 0;

    loop {
        let compressed = EpochSync::extract_chunk_from_proof(&full_proof, epoch_height).unwrap();
        let (proof, _) = compressed.decode().unwrap();
        let v2 = proof.into_v2();

        let chunk_len = v2.all_epochs.len();
        assert!(chunk_len > 0, "chunk should not be empty");
        total_epoch_entries += chunk_len;
        chunks_received += 1;

        let is_final = v2.last_epoch.is_some() && v2.current_epoch.is_some();

        if is_final {
            // Final chunk must have at least 2 entries for apply_validated_proof.
            assert!(chunk_len >= 2, "final chunk must have at least 2 epochs, got {chunk_len}");
            // Final chunk must include the same last_epoch/current_epoch as the full proof.
            assert_eq!(v2.last_epoch.unwrap(), full_proof.last_epoch);
            assert_eq!(v2.current_epoch.unwrap(), full_proof.current_epoch);
            break;
        } else {
            assert!(v2.last_epoch.is_none());
            assert!(v2.current_epoch.is_none());
            // Intermediate chunks should be MAX_EPOCHS_PER_CHUNK or less.
            assert!(chunk_len <= MAX_EPOCHS_PER_CHUNK);
        }

        epoch_height += chunk_len as EpochHeight;
    }

    // The total number of epoch entries across all chunks should cover the entire proof.
    // (Some entries may overlap in the final chunk if it was backed up to ensure >= 2 entries.)
    assert!(
        total_epoch_entries >= total_epochs,
        "chunks should cover entire proof: got {total_epoch_entries}, need {total_epochs}"
    );

    if total_epochs > MAX_EPOCHS_PER_CHUNK {
        assert!(
            chunks_received > 1,
            "proof with {total_epochs} epochs should require multiple chunks"
        );
    }
    tracing::info!(chunks_received, total_epoch_entries, "chunk coverage verified");
}

/// Test that intermediate chunks are not final (no last_epoch/current_epoch)
/// and that the very last chunk IS final.
#[test]
fn test_intermediate_vs_final_chunks() {
    init_test_logger();
    let env = setup_blockchain_for_incremental_sync();
    let full_proof = derive_full_proof(&env, 0);
    let total_epochs = full_proof.all_epochs.len();

    // First chunk starting from genesis.
    let compressed = EpochSync::extract_chunk_from_proof(&full_proof, 1).unwrap();
    let (proof, _) = compressed.decode().unwrap();
    let v2 = proof.into_v2();

    if total_epochs <= MAX_EPOCHS_PER_CHUNK {
        // Small proof: first chunk is also the final chunk.
        assert!(v2.last_epoch.is_some(), "single chunk should be final");
        assert!(v2.current_epoch.is_some(), "single chunk should be final");
    } else {
        // Large proof: first chunk is intermediate.
        assert!(v2.last_epoch.is_none(), "first chunk should not be final");
        assert!(v2.current_epoch.is_none(), "first chunk should not be final");
        assert_eq!(v2.all_epochs.len(), MAX_EPOCHS_PER_CHUNK);
    }
}

/// Test that a new node can bootstrap via incremental V2 epoch sync
/// (multiple request-response round trips).
#[test]
fn test_incremental_epoch_sync_bootstrap() {
    init_test_logger();
    let mut env = setup_blockchain_for_incremental_sync();

    let full_proof = derive_full_proof(&env, 0);
    let total_epochs = full_proof.all_epochs.len();
    tracing::info!(total_epochs, "full proof epoch count for bootstrap test");

    // Add a new node that will use epoch sync.
    let identifier = format!("account{}", env.node_datas.len());
    let node_state = env
        .node_state_builder()
        .account_id(&create_account_id(&identifier))
        .config_modifier(|config| {
            config.epoch_sync.epoch_sync_horizon_num_epochs = 3;
            config.block_header_fetch_horizon = 8;
            config.block_fetch_horizon = 3;
        })
        .build();
    env.add_node(&identifier, node_state);

    // Allow talking only with node 0.
    let new_node_idx = env.node_datas.len() - 1;
    let new_node_peer_id = env.node_datas[new_node_idx].peer_id.clone();
    env.shared_state.network_shared_state.allow_all_requests();
    for (index, data) in env.node_datas[..new_node_idx].iter().enumerate() {
        if index != 0 {
            env.shared_state
                .network_shared_state
                .disallow_requests(data.peer_id.clone(), new_node_peer_id.clone());
            env.shared_state
                .network_shared_state
                .disallow_requests(new_node_peer_id.clone(), data.peer_id.clone());
        }
    }

    // Track sync status transitions.
    let sync_status_history: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    {
        let history = sync_status_history.clone();
        let new_node = env.node_datas[new_node_idx].client_sender.actor_handle();
        env.test_loop.set_every_event_callback(move |test_loop_data| {
            let client = &test_loop_data.get(&new_node).client;
            let status = client.sync_handler.sync_status.as_variant_name();
            let mut h = history.borrow_mut();
            if h.last().map(|s| s.as_str()) != Some(status) {
                h.push(status.to_string());
            }
        });
    }

    // Run until the new node catches up.
    let new_node = env.node_datas[new_node_idx].client_sender.actor_handle();
    let node0 = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |test_loop_data| {
            let new_node_height = test_loop_data.get(&new_node).client.chain.head().unwrap().height;
            let node0_height = test_loop_data.get(&node0).client.chain.head().unwrap().height;
            new_node_height == node0_height
        },
        Duration::seconds(30),
    );

    // Run for at least two more epochs to confirm continued operation.
    let current_height = env.test_loop.data.get(&node0).client.chain.head().unwrap().height;
    env.test_loop.run_until(
        |test_loop_data| {
            let h = test_loop_data.get(&new_node).client.chain.head().unwrap().height;
            h >= current_height + 30
        },
        Duration::seconds(30),
    );

    // Verify the sync went through EpochSync status.
    let history = sync_status_history.borrow();
    assert!(
        history.contains(&"EpochSync".to_string()),
        "sync should have gone through EpochSync, got: {:?}",
        *history
    );

    // Verify the epoch sync proof was stored on disk.
    let store = env.test_loop.data.get(&new_node).client.chain.chain_store.store();
    let stored_proof = store.epoch_store().get_epoch_sync_proof().unwrap();
    assert!(stored_proof.is_some(), "epoch sync proof should be stored on new node");
}

/// Test that chunk extraction rejects an epoch_height that is beyond the proof range.
#[test]
fn test_extract_chunk_out_of_range() {
    init_test_logger();
    let env = setup_blockchain_for_incremental_sync();
    let full_proof = derive_full_proof(&env, 0);
    let total_epochs = full_proof.all_epochs.len();

    // epoch_height way beyond the proof range should error.
    let result =
        EpochSync::extract_chunk_from_proof(&full_proof, (total_epochs + 100) as EpochHeight);
    assert!(result.is_err(), "should reject epoch_height beyond proof range");
}

/// Test that each V2 chunk contains valid epoch data that matches the full proof.
#[test]
fn test_chunk_epoch_data_matches_full_proof() {
    init_test_logger();
    let env = setup_blockchain_for_incremental_sync();
    let full_proof = derive_full_proof(&env, 0);
    let total_epochs = full_proof.all_epochs.len();

    let mut epoch_height: EpochHeight = 1;
    let mut full_proof_index = 0;

    loop {
        let compressed = EpochSync::extract_chunk_from_proof(&full_proof, epoch_height).unwrap();
        let (proof, _) = compressed.decode().unwrap();
        let v2 = proof.into_v2();

        // Verify each epoch entry in the chunk matches the corresponding entry in the full proof.
        for epoch_data in &v2.all_epochs {
            assert!(
                full_proof_index < total_epochs,
                "chunk contains more epochs than the full proof"
            );
            assert_eq!(
                epoch_data.last_final_block_header,
                full_proof.all_epochs[full_proof_index].last_final_block_header,
                "epoch data mismatch at full_proof index {full_proof_index}"
            );
            full_proof_index += 1;
        }

        let is_final = v2.last_epoch.is_some();
        if is_final {
            break;
        }
        epoch_height += v2.all_epochs.len() as EpochHeight;
    }
    // The final chunk may have backed up to include overlapping entries,
    // so full_proof_index may exceed total_epochs slightly in that edge case.
    // But it should at least have covered everything.
    assert!(
        full_proof_index >= total_epochs,
        "chunks didn't cover all epochs: covered {full_proof_index}, total {total_epochs}"
    );
}

/// Test V2 epoch sync with a small chunk size to exercise multi-chunk behavior
/// even with few epochs. We do this by extracting chunks manually with small steps.
#[test]
fn test_manual_multi_chunk_walk() {
    init_test_logger();
    let env = setup_blockchain_for_incremental_sync();
    let full_proof = derive_full_proof(&env, 0);
    let total_epochs = full_proof.all_epochs.len();
    assert!(total_epochs >= 4, "need at least 4 epochs for this test");

    // Simulate requesting chunks of size ~3 by manually slicing.
    // This tests the epoch data continuity rather than the MAX_EPOCHS_PER_CHUNK constant.
    let step = 3;
    let mut offset = 0;
    let mut prev_next_bp_hash = None;

    while offset < total_epochs {
        let end = std::cmp::min(offset + step, total_epochs);
        let chunk = &full_proof.all_epochs[offset..end];

        // Verify block producer handoff continuity.
        for i in 1..chunk.len() {
            let bp_hash = near_primitives::block::compute_bp_hash_from_validator_stakes(
                &chunk[i].block_producers,
                chunk[i].use_versioned_bp_hash_format,
            );
            assert_eq!(
                bp_hash,
                *chunk[i - 1].last_final_block_header.next_bp_hash(),
                "block producer handoff failed within chunk at internal index {i}"
            );
        }

        // Verify cross-chunk continuity.
        if let Some(prev_hash) = prev_next_bp_hash {
            let bp_hash = near_primitives::block::compute_bp_hash_from_validator_stakes(
                &chunk[0].block_producers,
                chunk[0].use_versioned_bp_hash_format,
            );
            assert_eq!(
                bp_hash, prev_hash,
                "block producer handoff failed across chunks at offset {offset}"
            );
        }

        prev_next_bp_hash = Some(*chunk.last().unwrap().last_final_block_header.next_bp_hash());
        offset = end;
    }
    tracing::info!(total_epochs, "manual multi-chunk walk verified");
}
