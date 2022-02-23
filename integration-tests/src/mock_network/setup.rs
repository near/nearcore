use crate::mock_network::MockPeerManagerActor;
use actix::{Actor, Addr, Arbiter, Recipient};
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
use near_chain_configs::GenesisConfig;
#[cfg(feature = "test_features")]
use near_client::AdversarialControls;
use near_client::{start_client, start_view_client, ClientActor, ViewClientActor};
use near_network::test_utils::NetworkRecipient;
use near_network::types::NetworkClientMessages;
use near_primitives::network::PeerId;
use near_store::create_store;
use near_telemetry::TelemetryActor;
use nearcore::{get_store_path, NearConfig, NightshadeRuntime};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[allow(dead_code)]
fn setup_runtime(home_dir: &Path, config: &NearConfig) -> Arc<NightshadeRuntime> {
    let path = get_store_path(home_dir);
    let store = create_store(&path);

    Arc::new(NightshadeRuntime::with_config(
        home_dir,
        store.clone(),
        config,
        config.client_config.trie_viewer_state_size_limit,
        config.client_config.max_gas_burnt_view,
    ))
}

#[allow(dead_code)]
fn setup_mock_peer_manager_actor(
    runtime: Arc<NightshadeRuntime>,
    client_addr: Recipient<NetworkClientMessages>,
    genesis_config: &GenesisConfig,
    chain_genesis: &ChainGenesis,
    block_production_delay: Duration,
) -> MockPeerManagerActor {
    let chain =
        Chain::new_for_view_client(runtime, chain_genesis, DoomslugThresholdMode::NoApprovals)
            .unwrap();
    MockPeerManagerActor::new(client_addr, genesis_config, chain, 0, block_production_delay)
}

#[allow(dead_code)]
fn setup_mock_network(
    client_home_dir: &Path,
    network_home_dir: &Path,
    config: &NearConfig,
) -> (Addr<MockPeerManagerActor>, Addr<ClientActor>, Addr<ViewClientActor>) {
    let client_runtime = setup_runtime(client_home_dir, &config);
    let mock_network_runtime = setup_runtime(network_home_dir, &config);

    let telemetry = TelemetryActor::new(config.telemetry_config.clone()).start();
    let chain_genesis = ChainGenesis::from(&config.genesis);

    let node_id = PeerId::new(config.network_config.public_key.clone().into());
    let network_adapter = Arc::new(NetworkRecipient::default());
    #[cfg(feature = "test_features")]
    let adv = Arc::new(std::sync::RwLock::new(AdversarialControls::default()));

    let block_production_delay = config.client_config.min_block_production_delay;
    let (client_actor, _) = start_client(
        config.client_config.clone(),
        chain_genesis.clone(),
        client_runtime.clone(),
        node_id,
        network_adapter.clone(),
        config.validator_signer.clone(),
        telemetry,
        #[cfg(feature = "test_features")]
        adv.clone(),
    );

    let view_client = start_view_client(
        None,
        chain_genesis.clone(),
        client_runtime.clone(),
        network_adapter.clone(),
        config.client_config.clone(),
        #[cfg(feature = "test_features")]
        adv.clone(),
    );

    let arbiter = Arbiter::new();
    let client_actor1 = client_actor.clone();
    let genesis_config = config.genesis.config.clone();
    let mock_network_actor =
        MockPeerManagerActor::start_in_arbiter(&arbiter.handle(), move |_ctx| {
            setup_mock_peer_manager_actor(
                mock_network_runtime,
                client_actor1.recipient(),
                &genesis_config,
                &chain_genesis,
                block_production_delay,
            )
        });
    network_adapter.set_recipient(mock_network_actor.clone().recipient());
    (mock_network_actor, client_actor, view_client)
}

#[cfg(test)]
mod test {
    use crate::mock_network::setup::setup_mock_network;
    use actix::{Actor, System};
    use futures::{future, FutureExt};
    use near_actix_test_utils::run_actix;
    use near_chain_configs::Genesis;
    use near_client::GetBlock;
    use near_logger_utils::init_integration_logger;
    use near_network::test_utils::{open_port, WaitOrTimeoutActor};
    use nearcore::config::GenesisExt;
    use nearcore::{load_test_config, start_with_config};

    // just a test to test that the basic mocknet setup works
    #[test]
    fn test_mocknet_basic() {
        init_integration_logger();

        // first set up a network with only one validator and generate some blocks
        let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
        let mut near_config = load_test_config("test0", open_port(), genesis.clone());
        near_config.client_config.min_num_peers = 0;

        let dir = tempfile::Builder::new().prefix("test0").tempdir().unwrap();
        let path1 = dir.path().clone();
        run_actix(async move {
            let nearcore::NearNode { view_client, .. } =
                start_with_config(path1, near_config).expect("start_with_config");

            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    actix::spawn(view_client.send(GetBlock::latest()).then(|res| {
                        let latest_height =
                            if let Ok(Ok(block)) = res { block.header.height } else { 0 };
                        if latest_height >= 20 {
                            System::current().stop()
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
        let dir1 = tempfile::Builder::new().prefix("test1").tempdir().unwrap();
        let mut near_config1 = load_test_config("test1", open_port(), genesis);
        near_config1.client_config.min_num_peers = 1;
        run_actix(async move {
            let (_mock_network, _client, view_client) =
                setup_mock_network(dir1.path().clone(), dir.path().clone(), &near_config1);
            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    actix::spawn(view_client.send(GetBlock::latest()).then(|res| {
                        if let Ok(Ok(block)) = res {
                            if block.header.height >= 20 {
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
        })
    }
}
