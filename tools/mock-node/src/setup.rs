//! Provides functions for setting up a mock network from configs and home dirs.

use crate::{MockNetworkConfig, MockPeer};
use anyhow::Context;
use near_chain::types::RuntimeAdapter;
use near_chain::ChainStoreUpdate;
use near_chain::{Chain, ChainGenesis, ChainStore, ChainStoreAccess, DoomslugThresholdMode};
use near_crypto::{KeyType, SecretKey};
use near_epoch_manager::shard_tracker::{ShardTracker, TrackedConfig};
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_jsonrpc_client::JsonRpcClient;
use near_network::tcp;
use near_network::types::PeerInfo;
use near_primitives::network::PeerId;
use near_primitives::state_part::PartId;
use near_primitives::syncing::get_num_state_parts;
use near_primitives::types::{BlockHeight, ShardId};
use near_store::test_utils::create_test_store;
use nearcore::{NearConfig, NightshadeRuntime};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::cmp::min;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn setup_runtime(
    home_dir: &Path,
    config: &NearConfig,
    in_memory_storage: bool,
) -> (Arc<EpochManagerHandle>, ShardTracker, Arc<NightshadeRuntime>) {
    let store = if in_memory_storage {
        create_test_store()
    } else {
        near_store::NodeStorage::opener(home_dir, config.config.archive, &config.config.store, None)
            .open()
            .unwrap()
            .get_hot_store()
    };
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &config.genesis.config);
    let shard_tracker =
        ShardTracker::new(TrackedConfig::from_config(&config.client_config), epoch_manager.clone());
    let runtime = NightshadeRuntime::from_config(home_dir, store, config, epoch_manager.clone());
    (epoch_manager, shard_tracker, runtime)
}

fn setup_mock_peer(
    chain: Chain,
    config: &mut NearConfig,
    network_start_height: Option<BlockHeight>,
    network_config: MockNetworkConfig,
    target_height: BlockHeight,
    num_shards: ShardId,
    mock_listen_addr: tcp::ListenerAddr,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let network_start_height = match network_start_height {
        None => target_height,
        Some(0) => chain.genesis_block().header().height(),
        Some(it) => it,
    };
    let secret_key = SecretKey::from_random(KeyType::ED25519);
    config
        .network_config
        .peer_store
        .boot_nodes
        .push(PeerInfo::new(PeerId::new(secret_key.public_key()), *mock_listen_addr));
    let chain_id = config.genesis.config.chain_id.clone();
    let block_production_delay = config.client_config.min_block_production_delay;
    let archival = config.client_config.archive;
    actix::spawn(async move {
        let mock = MockPeer::new(
            chain,
            secret_key,
            mock_listen_addr,
            chain_id,
            archival,
            block_production_delay,
            num_shards,
            network_start_height,
            network_config,
        )
        .await?;
        mock.run(target_height).await
    })
}

pub struct MockNode {
    // target height actually available to sync to in the chain history database
    pub target_height: BlockHeight,
    pub mock_peer: tokio::task::JoinHandle<anyhow::Result<()>>,
    // client that allows making RPC requests to the node under test
    pub rpc_client: JsonRpcClient,
}

/// Setup up a mock node, including setting up
/// a MockPeerManagerActor and a ClientActor and a ViewClientActor
/// `client_home_dir`: home dir for the new client
/// `network_home_dir`: home dir that contains the pre-generated chain history, will be used
///                     to construct `MockPeerManagerActor`
/// `config`: config for the new client
/// `network_delay`: delay for getting response from the simulated network
/// `client_start_height`: start height for client
/// `network_start_height`: height at which the simulated network starts producing blocks
/// `target_height`: height that the simulated peers will produce blocks until. If None, will
///                  use the height from the chain head in storage
/// `in_memory_storage`: if true, make client use in memory storage instead of rocksdb
///
/// Returns a struct representing the node under test
pub fn setup_mock_node(
    client_home_dir: &Path,
    network_home_dir: &Path,
    mut config: NearConfig,
    network_config: &MockNetworkConfig,
    client_start_height: BlockHeight,
    network_start_height: Option<BlockHeight>,
    target_height: Option<BlockHeight>,
    in_memory_storage: bool,
    mock_listen_addr: tcp::ListenerAddr,
) -> MockNode {
    let parent_span = tracing::debug_span!(target: "mock_node", "setup_mock_node").entered();
    let (mock_network_epoch_manager, mock_network_shard_tracker, mock_network_runtime) =
        setup_runtime(network_home_dir, &config, false);

    let chain_genesis = ChainGenesis::new(&config.genesis);

    // set up client dir to be ready to process blocks from client_start_height
    if client_start_height > 0 {
        tracing::info!(target: "mock_node", "Preparing client data dir to be able to start at the specified start height {}", client_start_height);
        let (client_epoch_manager, _, client_runtime) =
            setup_runtime(client_home_dir, &config, in_memory_storage);
        let mut chain_store = ChainStore::new(
            client_runtime.store().clone(),
            config.genesis.config.genesis_height,
            config.client_config.save_trie_changes,
        );
        let mut network_chain_store = ChainStore::new(
            mock_network_runtime.store().clone(),
            config.genesis.config.genesis_height,
            config.client_config.save_trie_changes,
        );

        let network_tail_height = network_chain_store.tail().unwrap();
        let network_head_height = network_chain_store.head().unwrap().height;
        tracing::info!(target: "mock_node",
              "network data chain tail height {} head height {}",
              network_tail_height,
              network_head_height,
        );
        assert!(
            client_start_height <= network_head_height
                && client_start_height >= network_tail_height,
            "client start height {} is not within the network chain range [{}, {}]",
            client_start_height,
            network_tail_height,
            network_head_height
        );
        let hash = network_chain_store.get_block_hash_by_height(client_start_height).unwrap();
        if !mock_network_epoch_manager.is_next_block_epoch_start(&hash).unwrap() {
            let epoch_start_height =
                mock_network_epoch_manager.get_epoch_start_height(&hash).unwrap();
            panic!(
                "start height must be the last block of an epoch, try using {} instead",
                epoch_start_height - 1
            );
        }

        // copy chain info
        let chain_store_update = ChainStoreUpdate::copy_chain_state_as_of_block(
            &mut chain_store,
            &hash,
            mock_network_epoch_manager.as_ref(),
            &mut network_chain_store,
        )
        .unwrap();
        chain_store_update.commit().unwrap();
        tracing::info!(target: "mock_node", "Done preparing chain state");

        client_epoch_manager
            .write()
            .copy_epoch_info_as_of_block(&hash, &mock_network_epoch_manager.read())
            .unwrap();
        tracing::info!(target: "mock_node", "Done preparing epoch info");

        // copy state for all shards
        let block = network_chain_store.get_block(&hash).unwrap();
        let prev_hash = *block.header().prev_hash();
        for (shard_id, chunk_header) in block.chunks().iter().enumerate() {
            tracing::info!(target: "mock_node", "Preparing state for shard {}", shard_id);
            let shard_id = shard_id as u64;
            let state_root = chunk_header.prev_state_root();
            let state_root_node =
                mock_network_runtime.get_state_root_node(shard_id, &hash, &state_root).unwrap();
            let num_parts = get_num_state_parts(state_root_node.memory_usage);
            let finished_parts_count = Arc::new(AtomicUsize::new(0));

            (0..num_parts)
                .into_par_iter()
                .try_for_each(|part_id| -> anyhow::Result<()> {
                    let _span = tracing::debug_span!(
                        target: "mock_node",
                        parent: &parent_span,
                        "obtain_and_apply_state_part",
                        part_id,
                        shard_id)
                    .entered();

                    let state_part = mock_network_runtime
                        .obtain_state_part(
                            shard_id,
                            &prev_hash,
                            &state_root,
                            PartId::new(part_id, num_parts),
                        )
                        .with_context(|| {
                            format!("Obtaining state part {} in shard {}", part_id, shard_id)
                        })?;
                    client_runtime
                        .apply_state_part(
                            shard_id,
                            &state_root,
                            PartId::new(part_id, num_parts),
                            &state_part,
                            &mock_network_epoch_manager.get_epoch_id_from_prev_block(&hash)?,
                        )
                        .with_context(|| {
                            format!("Applying state part {} in shard {}", part_id, shard_id)
                        })?;
                    finished_parts_count.fetch_add(1, Ordering::SeqCst);
                    tracing::info!(
                        target: "mock_node",
                        "Done {}/{} parts for shard {}",
                        finished_parts_count.load(Ordering::SeqCst),
                        num_parts,
                        shard_id,
                    );
                    Ok(())
                })
                .unwrap();
        }
    }

    let chain = Chain::new_for_view_client(
        mock_network_epoch_manager.clone(),
        mock_network_shard_tracker,
        mock_network_runtime,
        &chain_genesis,
        DoomslugThresholdMode::NoApprovals,
        config.client_config.save_trie_changes,
    )
    .unwrap();
    let head = chain.head().unwrap();
    let target_height = min(target_height.unwrap_or(head.height), head.height);
    let num_shards = mock_network_epoch_manager.num_shards(&head.epoch_id).unwrap();

    config.network_config.peer_store.boot_nodes.clear();
    let mock_peer = setup_mock_peer(
        chain,
        &mut config,
        network_start_height,
        network_config.clone(),
        target_height,
        num_shards,
        mock_listen_addr,
    );

    let rpc_client = near_jsonrpc_client::new_client(&format!(
        "http://{}",
        &config.rpc_config.as_ref().expect("the JSON RPC config must be set").addr
    ));
    let _node = nearcore::start_with_config(client_home_dir, config).unwrap();

    MockNode { target_height, mock_peer, rpc_client }
}

#[cfg(test)]
mod tests {
    use crate::setup::{setup_mock_node, MockNode};
    use crate::MockNetworkConfig;
    use actix::{Actor, System};
    use futures::{future, FutureExt};
    use near_actix_test_utils::{run_actix, spawn_interruptible};
    use near_chain::{ChainStore, ChainStoreAccess};
    use near_chain_configs::Genesis;
    use near_client::{GetBlock, ProcessTxRequest};
    use near_crypto::{InMemorySigner, KeyType};
    use near_epoch_manager::{EpochManager, EpochManagerAdapter};
    use near_network::tcp;
    use near_network::test_utils::{wait_or_timeout, WaitOrTimeoutActor};
    use near_o11y::testonly::init_integration_logger;
    use near_o11y::WithSpanContextExt;
    use near_primitives::transaction::SignedTransaction;
    use near_store::test_utils::gen_account;
    use nearcore::config::GenesisExt;
    use nearcore::{load_test_config, start_with_config, NEAR_BASE};
    use rand::thread_rng;
    use std::ops::ControlFlow;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    // Just a test to test that the basic mocknet setup works
    // This test first starts a localnet with one validator node that generates 20 blocks
    // to generate a chain history
    // then start a mock network with this chain history and test that
    // the client in the mock network can catch up these 20 blocks
    #[cfg_attr(not(feature = "mock_node"), ignore)]
    #[test]
    fn test_mock_node_basic() {
        init_integration_logger();

        // first set up a network with only one validator and generate some blocks
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        let epoch_length = 40;
        genesis.config.epoch_length = epoch_length;
        let mut near_config =
            load_test_config("test0", tcp::ListenerAddr::reserve_for_test(), genesis.clone());
        near_config.client_config.min_num_peers = 0;
        near_config.config.state_sync_enabled = Some(true);
        near_config.config.store.state_snapshot_enabled = true;

        let dir = tempfile::Builder::new().prefix("test0").tempdir().unwrap();
        let path1 = dir.path();
        run_actix(async move {
            let nearcore::NearNode { view_client, client, .. } =
                start_with_config(path1, near_config).expect("start_with_config");

            let view_client1 = view_client;
            let nonce = Arc::new(RwLock::new(10));
            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    let nonce = nonce.clone();
                    let client1 = client.clone();
                    let actor = view_client1.send(GetBlock::latest().with_span_context());
                    let actor = actor.then(move |res| {
                        if let Ok(Ok(block)) = res {
                            let next_nonce = *nonce.read().unwrap();
                            if next_nonce < 100 {
                                WaitOrTimeoutActor::new(
                                    Box::new(move |_ctx| {
                                        let signer0 = InMemorySigner::from_seed(
                                            "test1".parse().unwrap(),
                                            KeyType::ED25519,
                                            "test1",
                                        );
                                        let mut rng = thread_rng();
                                        let transaction = SignedTransaction::create_account(
                                            next_nonce,
                                            "test1".parse().unwrap(),
                                            gen_account(&mut rng, b"abcdefghijklmn")
                                                .parse()
                                                .unwrap(),
                                            5 * NEAR_BASE,
                                            signer0.public_key.clone(),
                                            &signer0,
                                            block.header.hash,
                                        );
                                        spawn_interruptible(
                                            client1
                                                .send(
                                                    ProcessTxRequest {
                                                        transaction,
                                                        is_forwarded: false,
                                                        check_only: false,
                                                    }
                                                    .with_span_context(),
                                                )
                                                .then(move |_res| future::ready(())),
                                        );
                                    }),
                                    100,
                                    30000,
                                )
                                .start();
                                *nonce.write().unwrap() = next_nonce + 1;
                            }

                            if block.header.height >= epoch_length * 2 + 2 {
                                tracing::info!(block_height = ?block.header.height, expected = epoch_length * 2 + 2, "Time to stop");
                                System::current().stop()
                            }
                        }
                        future::ready(())
                    });
                    spawn_interruptible(actor);
                }),
                100,
                60000,
            )
            .start();
        });

        // start the mock network to simulate a new node "test1" to sync up
        // start the client at height 10 (end of the first epoch)
        let dir1 = tempfile::Builder::new().prefix("test1").tempdir().unwrap();
        let mut near_config1 = load_test_config("", tcp::ListenerAddr::reserve_for_test(), genesis);
        near_config1.client_config.min_num_peers = 1;
        near_config1.client_config.tracked_shards = vec![0]; // Track all shards.
        near_config1.config.state_sync_enabled = Some(true);
        near_config1.config.store.state_snapshot_enabled = true;
        near_config1.config.store.state_snapshot_compaction_enabled = false;
        let network_config = MockNetworkConfig::with_delay(Duration::from_millis(10));

        let client_start_height = {
            let store = near_store::NodeStorage::opener(
                dir.path(),
                near_config1.config.archive,
                &near_config1.config.store,
                None,
            )
            .open()
            .unwrap()
            .get_hot_store();
            let epoch_manager =
                EpochManager::new_arc_handle(store.clone(), &near_config1.genesis.config);
            let chain_store = ChainStore::new(
                store,
                near_config1.genesis.config.genesis_height,
                near_config1.client_config.save_trie_changes,
            );
            let network_head_hash = chain_store.head().unwrap().last_block_hash;
            let last_epoch_start_height =
                epoch_manager.get_epoch_start_height(&network_head_hash).unwrap();
            // Needs to be the last block of an epoch.
            last_epoch_start_height - 1
        };

        run_actix(async {
            let MockNode { rpc_client, .. } = setup_mock_node(
                dir1.path(),
                dir.path(),
                near_config1,
                &network_config,
                client_start_height,
                None,
                None,
                false,
                tcp::ListenerAddr::reserve_for_test(),
            );
            wait_or_timeout(100, 60000, || async {
                if let Ok(status) = rpc_client.status().await {
                    if status.sync_info.latest_block_height >= client_start_height {
                        System::current().stop();
                        return ControlFlow::Break(());
                    }
                }
                ControlFlow::Continue(())
            })
            .await
            .unwrap();
        })
    }
}
