//! Provides functions for setting up a mock network from configs and home dirs.

use crate::{MockNet, MockNetworkConfig};
use anyhow::Context;
use near_chain::ChainStoreUpdate;
use near_chain::{
    Chain, ChainGenesis, ChainStore, ChainStoreAccess, DoomslugThresholdMode, RuntimeAdapter,
};
use near_epoch_manager::EpochManager;
use near_primitives::state_part::PartId;
use near_primitives::syncing::get_num_state_parts;
use near_primitives::types::BlockHeight;
use near_store::test_utils::create_test_store;
use nearcore::{NearConfig, NightshadeRuntime};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::cmp::min;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn setup_runtime(
    home_dir: &Path,
    config: &NearConfig,
    in_memory_storage: bool,
) -> Arc<NightshadeRuntime> {
    let store = if in_memory_storage {
        create_test_store()
    } else {
        nearcore::init_and_migrate_store(home_dir, config).unwrap()
    };

    Arc::new(NightshadeRuntime::from_config(home_dir, store, config))
}

fn setup_mock_network(
    chain: Chain,
    config: &mut NearConfig,
    block_production_delay: Duration,
    client_start_height: BlockHeight,
    network_start_height: Option<BlockHeight>,
    network_config: &MockNetworkConfig,
    target_height: BlockHeight,
) -> MockNet {
    let network_start_height = match network_start_height {
        None => target_height,
        Some(0) => chain.genesis_block().header().height(),
        Some(it) => it,
    };
    MockNet::new(
        config,
        chain,
        client_start_height,
        network_start_height,
        target_height,
        block_production_delay,
        network_config,
    )
}

pub struct MockNode {
    // mock HTTP server that will respond to RPC/status requests
    pub server: Option<near_jsonrpc::mock::MockHttpServer>,
    // target height actually available to sync to in the chain history database
    pub target_height: BlockHeight,
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
) -> MockNode {
    let parent_span = tracing::debug_span!(target: "mock_node", "setup_mock_node").entered();
    let mock_network_runtime = setup_runtime(network_home_dir, &config, false);

    let chain_genesis = ChainGenesis::from(&config.genesis);

    // set up client dir to be ready to process blocks from client_start_height
    if client_start_height > 0 {
        let client_runtime = setup_runtime(client_home_dir, &config, in_memory_storage);
        tracing::info!(target: "mock_node", "Preparing client data dir to be able to start at the specified start height {}", client_start_height);
        let mut chain_store = ChainStore::new(
            client_runtime.get_store(),
            config.genesis.config.genesis_height,
            !config.client_config.archive,
        );
        let mut network_chain_store = ChainStore::new(
            mock_network_runtime.get_store(),
            config.genesis.config.genesis_height,
            !config.client_config.archive,
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
        if !mock_network_runtime.is_next_block_epoch_start(&hash).unwrap() {
            let epoch_start_height = mock_network_runtime.get_epoch_start_height(&hash).unwrap();
            panic!(
                "start height must be the last block of an epoch, try using {} instead",
                epoch_start_height - 1
            );
        }

        // copy chain info
        let chain_store_update = ChainStoreUpdate::copy_chain_state_as_of_block(
            &mut chain_store,
            &hash,
            mock_network_runtime.clone(),
            &mut network_chain_store,
        )
        .unwrap();
        chain_store_update.commit().unwrap();
        tracing::info!(target: "mock_node", "Done preparing chain state");

        // copy epoch info
        let mut epoch_manager = EpochManager::new_from_genesis_config(
            client_runtime.get_store(),
            &config.genesis.config,
        )
        .unwrap();
        let mut mock_epoch_manager = EpochManager::new_from_genesis_config(
            mock_network_runtime.get_store(),
            &config.genesis.config,
        )
        .unwrap();
        epoch_manager.copy_epoch_info_as_of_block(&hash, &mut mock_epoch_manager).unwrap();
        tracing::info!(target: "mock_node", "Done preparing epoch info");

        // copy state for all shards
        let next_hash = network_chain_store.get_next_block_hash(&hash).unwrap();
        let next_block = network_chain_store.get_block(&next_hash).unwrap();
        for (shard_id, chunk_header) in next_block.chunks().iter().enumerate() {
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
                            &next_hash,
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
                            &mock_network_runtime.get_epoch_id_from_prev_block(&hash)?,
                        )
                        .with_context(|| {
                            format!("Applying state part {} in shard {}", part_id, shard_id)
                        })?;
                    finished_parts_count.fetch_add(1, Ordering::SeqCst);
                    tracing::info!(
                        target: "mock_node",
                        "Done {}/{} parts for shard {}",
                        finished_parts_count.load(Ordering::SeqCst) + 1,
                        num_parts,
                        shard_id,
                    );
                    Ok(())
                })
                .unwrap();
        }
    }

    let block_production_delay = config.client_config.min_block_production_delay;
    let archival = config.client_config.archive;
    let network_config = network_config.clone();

    let chain = Chain::new_for_view_client(
        mock_network_runtime,
        &chain_genesis,
        DoomslugThresholdMode::NoApprovals,
        !archival,
    )
    .unwrap();
    let chain_height = chain.head().unwrap().height;
    let target_height = min(target_height.unwrap_or(chain_height), chain_height);

    let mock = setup_mock_network(
        chain,
        &mut config,
        block_production_delay,
        client_start_height,
        network_start_height,
        &network_config,
        target_height,
    );

    let server = nearcore::start_mock(client_home_dir, &config, Box::new(mock)).unwrap();

    MockNode { server, target_height }
}

#[cfg(test)]
mod tests {
    use crate::setup::{setup_mock_node, MockNode};
    use crate::MockNetworkConfig;
    use actix::{Actor, System};
    use futures::{future, FutureExt};
    use near_actix_test_utils::{run_actix, spawn_interruptible};
    use near_chain_configs::Genesis;
    use near_client::GetBlock;
    use near_crypto::{InMemorySigner, KeyType};
    use near_logger_utils::init_integration_logger;
    use near_network::test_utils::wait_or_timeout;
    use near_network::test_utils::{open_port, WaitOrTimeoutActor};
    use near_network::types::NetworkClientMessages;
    use near_primitives::hash::CryptoHash;
    use near_primitives::transaction::SignedTransaction;
    use near_store::test_utils::gen_account;
    use nearcore::config::GenesisExt;
    use nearcore::{load_test_config, start_with_config, NEAR_BASE};
    use rand::thread_rng;
    use serde_json::Value;
    use std::ops::ControlFlow;
    use std::str::FromStr;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    fn json_block_hash(block: &Value) -> anyhow::Result<CryptoHash> {
        match crate::utils::json_block_header(block)?.get("hash") {
            Some(h) => match h {
                Value::String(s) => match CryptoHash::from_str(s) {
                    Ok(h) => Ok(h),
                    Err(_) => Err(anyhow::anyhow!("get_latest_block() header has bad hash: {}", h)),
                },
                _ => Err(anyhow::anyhow!("get_latest_block() header has bad hash: {}", h)),
            },
            None => Err(anyhow::anyhow!("get_latest_block() header has no hash")),
        }
    }

    // Just a test to test that the basic mocknet setup works
    // This test first starts a localnet with one validator node that generates 20 blocks
    // to generate a chain history
    // then start a mock network with this chain history and test that
    // the client in the mock network can catch up these 20 blocks
    #[test]
    fn test_mock_node_basic() {
        init_integration_logger();

        // first set up a network with only one validator and generate some blocks
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = 10;
        let mut near_config = load_test_config("test0", open_port(), genesis.clone());
        near_config.client_config.min_num_peers = 0;

        let dir = tempfile::Builder::new().prefix("test0").tempdir().unwrap();
        let path1 = dir.path().clone();
        let last_block = Arc::new(RwLock::new(CryptoHash::default()));
        let last_block1 = last_block.clone();
        run_actix(async move {
            let nearcore::NearNode { view_client, client, .. } =
                start_with_config(path1, near_config).expect("start_with_config");

            let view_client1 = view_client;
            let nonce = Arc::new(RwLock::new(10));
            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    let last_block2 = last_block1.clone();
                    let nonce = nonce.clone();
                    let client1 = client.clone();
                    spawn_interruptible(view_client1.send(GetBlock::latest()).then(move |res| {
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
                                                .send(NetworkClientMessages::Transaction {
                                                    transaction,
                                                    is_forwarded: false,
                                                    check_only: false,
                                                })
                                                .then(move |_res| future::ready(())),
                                        );
                                    }),
                                    100,
                                    30000,
                                )
                                .start();
                                *nonce.write().unwrap() = next_nonce + 1;
                            }

                            if block.header.height >= 20 {
                                *last_block2.write().unwrap() = block.header.hash;
                                System::current().stop()
                            }
                        }
                        future::ready(())
                    }));
                }),
                100,
                60000,
            )
            .start();
        });

        // start the mock network to simulate a new node "test1" to sync up
        // start the client at height 10 (end of the first epoch)
        let dir1 = tempfile::Builder::new().prefix("test1").tempdir().unwrap();
        let mut near_config1 = load_test_config("", open_port(), genesis);
        near_config1.client_config.min_num_peers = 1;
        near_config1.client_config.tracked_shards =
            (0..near_config1.genesis.config.shard_layout.num_shards()).collect();
        let network_config = MockNetworkConfig::with_delay(Duration::from_millis(10));
        run_actix(async move {
            let MockNode { server, .. } = setup_mock_node(
                dir1.path().clone(),
                dir.path().clone(),
                near_config1,
                &network_config,
                10,
                None,
                None,
                false,
            );
            let server = server.unwrap();
            wait_or_timeout(100, 60000, || async {
                match server.get_latest_block().await {
                    Ok(block) => {
                        if crate::utils::json_block_height(&block).unwrap() >= 20 {
                            assert_eq!(
                                *last_block.read().unwrap(),
                                json_block_hash(&block).unwrap()
                            );
                            System::current().stop();
                            ControlFlow::Break(())
                        } else {
                            ControlFlow::Continue(())
                        }
                    },
                    Err(e) => {
                        tracing::error!(target: "mock-node", "can't get latest height from RPC: {:?}", e);
                        ControlFlow::Continue(())
                    }
                }
            }).await.unwrap();
        })
    }
}
