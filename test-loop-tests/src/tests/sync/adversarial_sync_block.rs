use super::util::{TEST_EPOCH_SYNC_HORIZON, far_horizon_height};
use crate::setup::builder::TestLoopBuilder;
use crate::setup::peer_manager_actor::HandlerResult;
use crate::utils::account::create_account_id;
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::CanSendAsync;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_client::{BlockResponse, SyncStatus};
use near_network::client::BlockRequest;
use near_network::types::{NetworkRequests, NetworkResponses};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Block;
use near_primitives::block_body::BlockBody;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::Balance;
use near_primitives::version::PROTOCOL_VERSION;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Replace every chunk header in `block`'s body, leaving the honest signed header untouched.
fn forge_block_body(block: &Block) -> Arc<Block> {
    let Block::BlockV4(mut v4) = block.clone() else {
        panic!("expected BlockV4");
    };
    let height = v4.header.height();
    let prev_hash = *v4.header.prev_hash();
    let BlockBody::V2(body) = &mut v4.body else {
        panic!("expected BlockBodyV2");
    };
    for chunk_header in &mut body.chunks {
        let mut forged = ShardChunkHeader::new_dummy(
            chunk_header.height_created(),
            chunk_header.shard_id(),
            prev_hash,
            PROTOCOL_VERSION,
        );
        *forged.height_included_mut() = height;
        *chunk_header = forged;
    }
    Arc::new(Block::BlockV4(v4))
}

// While a node is in StateSync it asks a peer for the sync-hash block,
// and that peer answers with the honest header and a forged body.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_forged_sync_block_body_is_rejected() {
    init_test_logger();

    let epoch_length = 10;
    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

    let victim_account = create_account_id("victim");
    let node_state = env
        .node_state_builder()
        .account_id(&victim_account)
        .config_modifier(|config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("victim", node_state);
    let victim_idx = env.node_datas.len() - 1;

    // The victim publishes its sync hash on entering StateSync. The interceptor needs it to
    // tell the sync-hash request from the prev/extra block requests: only the sync-hash block
    // goes to the orphan pool, and only the orphan pool body is ever read back.
    let sync_hash: Arc<Mutex<Option<CryptoHash>>> = Arc::new(Mutex::new(None));
    let sync_hash_cell = sync_hash.clone();
    let sync_hash_reader = sync_hash.clone();

    // Chunk headers of the forged body, recorded just before it is handed to the victim.
    let forged_chunks: Arc<Mutex<Option<Vec<ShardChunkHeader>>>> = Arc::new(Mutex::new(None));
    let forged_chunks_reader = forged_chunks.clone();

    // Set if the forged body is ever seen inside the victim's orphan pool.
    let forgery_stored = Arc::new(AtomicBool::new(false));
    let forgery_stored_writer = forgery_stored.clone();

    let victim_handle = env.node_datas[victim_idx].client_sender.actor_handle();
    let callback_handle = victim_handle.clone();
    env.test_loop.set_every_event_callback(move |data| {
        let client = &data.get(&callback_handle).client;
        if let SyncStatus::StateSync(status) = &client.sync_handler.sync_status {
            *sync_hash_cell.lock() = Some(status.sync_hash);
        }

        let Some(sync_hash) = *sync_hash_cell.lock() else { return };
        let Some(orphan) = client.chain.get_orphan(&sync_hash) else { return };
        let forged_chunks = forged_chunks_reader.lock();
        let Some(forged_chunks) = forged_chunks.as_ref() else { return };
        if orphan.chunks().iter_raw().eq(forged_chunks.iter()) {
            forgery_stored_writer.store(true, Ordering::SeqCst);
        }
    });

    // Forge the first sync-hash request the victim makes. Later requests
    // fall through to the honest default handler, which is what lets a node that *rejects* the
    // forgery recover.
    let forged_delivered = Arc::new(AtomicUsize::new(0));
    let forged_counter = forged_delivered.clone();
    let forged_once = Arc::new(AtomicBool::new(false));

    // Stand in for the peer the victim asks for its sync blocks.
    let network_shared_state = env.shared_state.network_shared_state.clone();
    let victim_data = &env.node_datas[victim_idx];
    let victim_account_id = victim_data.account_id.clone();
    let future_spawner: Arc<dyn FutureSpawner> =
        Arc::new(env.test_loop.future_spawner(&victim_data.identifier));
    victim_data.register_override_handler(
        &mut env.test_loop.data,
        Box::new(move |request| match request {
            NetworkRequests::BlockRequest { hash, peer_id }
                if Some(hash) == *sync_hash.lock() && !forged_once.swap(true, Ordering::SeqCst) =>
            {
                let my_peer_id = network_shared_state.account_to_peer_id(&victim_account_id);
                let responder = network_shared_state
                    .senders_for_peer(&peer_id, &my_peer_id)
                    .client_sender
                    .clone();
                let future = network_shared_state
                    .senders_for_peer(&my_peer_id, &peer_id)
                    .view_client_sender
                    .send_async(BlockRequest(hash));
                let forged_counter = forged_counter.clone();
                let forged_chunks = forged_chunks.clone();
                future_spawner.spawn("forged sync block response", async move {
                    let Ok(Some(block)) = future.await else { return };
                    let forged = forge_block_body(&block);
                    // The whole point: the forgery keeps the honest hash, so a node that
                    // accepts it stores it under the hash it is waiting for.
                    assert_eq!(forged.hash(), block.hash());
                    *forged_chunks.lock() =
                        Some(forged.chunks().iter_raw().cloned().collect::<Vec<_>>());
                    forged_counter.fetch_add(1, Ordering::SeqCst);
                    let future = responder.send_async(
                        BlockResponse { block: forged, peer_id, was_requested: true }.span_wrap(),
                    );
                    drop(future);
                });
                HandlerResult::Handled(NetworkResponses::NoResponse)
            }
            other => HandlerResult::Unhandled(other),
        }),
    );

    // Let the victim reject the forgery, wait out `block_request_timeout` (100 ms), re-request
    // from another peer and finish syncing.
    let source_handle = env.node_datas[0].client_sender.actor_handle();
    let head_handle = victim_handle;
    let forgery_stored_reader = forgery_stored.clone();
    env.test_loop.run_until(
        |data| {
            // Stop early on a stored forgery so the failure is reported instead of being
            // buried in whatever the victim does next.
            if forgery_stored_reader.load(Ordering::SeqCst) {
                return true;
            }
            let victim_height = data.get(&head_handle).client.chain.head().unwrap().height;
            let source_height = data.get(&source_handle).client.chain.head().unwrap().height;
            victim_height == source_height
        },
        Duration::seconds(600),
    );

    assert!(
        forged_delivered.load(Ordering::SeqCst) > 0,
        "no forged sync block was delivered; the victim never reached state sync"
    );

    let sync_hash = sync_hash_reader.lock().expect("victim never entered state sync");
    assert!(
        !forgery_stored.load(Ordering::SeqCst),
        "forged sync block was stored in the orphan pool under the honest hash {sync_hash}"
    );
}
