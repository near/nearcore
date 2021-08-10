use std::sync::Arc;

use actix::Addr;
use actix_rt::ArbiterHandle;
use tempfile::{tempdir, TempDir};

use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
use near_chain_configs::Genesis;
use near_client::{ClientActor, ViewClientActor};
use near_logger_utils::init_integration_logger;
use near_network::test_utils::{convert_boot_nodes, open_port};
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, BlockHeightDelta, NumSeats, NumShards};
use near_store::test_utils::create_test_store;
use nearcore::{config::GenesisExt, load_test_config, start_with_config, NightshadeRuntime};

pub mod fees_utils;
pub mod node;
pub mod runtime_utils;
pub mod test_helpers;
pub mod user;

/// Compute genesis hash from genesis.
pub fn genesis_hash(genesis: &Genesis) -> CryptoHash {
    *genesis_header(genesis).hash()
}

/// Utility to generate genesis header from config for testing purposes.
pub fn genesis_header(genesis: &Genesis) -> BlockHeader {
    let dir = tempdir().unwrap();
    let store = create_test_store();
    let chain_genesis = ChainGenesis::from(genesis);
    let runtime = Arc::new(NightshadeRuntime::default(dir.path(), store, genesis));
    let chain = Chain::new(runtime, &chain_genesis, DoomslugThresholdMode::TwoThirds).unwrap();
    chain.genesis().clone()
}

/// Utility to generate genesis header from config for testing purposes.
pub fn genesis_block(genesis: &Genesis) -> Block {
    let dir = tempdir().unwrap();
    let store = create_test_store();
    let chain_genesis = ChainGenesis::from(genesis);
    let runtime = Arc::new(NightshadeRuntime::default(dir.path(), store, genesis));
    let mut chain = Chain::new(runtime, &chain_genesis, DoomslugThresholdMode::TwoThirds).unwrap();
    chain.get_block(&chain.genesis().hash().clone()).unwrap().clone()
}

pub fn start_nodes(
    num_shards: NumShards,
    dirs: &[TempDir],
    num_validator_seats: NumSeats,
    num_lightclient: usize,
    epoch_length: BlockHeightDelta,
    genesis_height: BlockHeight,
) -> (Genesis, Vec<String>, Vec<(Addr<ClientActor>, Addr<ViewClientActor>, Vec<ArbiterHandle>)>) {
    init_integration_logger();

    let num_nodes = dirs.len();
    let num_tracking_nodes = num_nodes - num_lightclient;
    let seeds = (0..num_nodes).map(|i| format!("near.{}", i)).collect::<Vec<_>>();
    let mut genesis = Genesis::test_sharded(
        seeds.iter().map(|s| s.parse().unwrap()).collect(),
        num_validator_seats,
        (0..num_shards).map(|_| num_validator_seats).collect(),
    );
    genesis.config.epoch_length = epoch_length;
    genesis.config.genesis_height = genesis_height;

    let validators = (0..num_validator_seats).map(|i| format!("near.{}", i)).collect::<Vec<_>>();
    let mut near_configs = vec![];
    let first_node = open_port();
    let mut rpc_addrs = vec![];
    for i in 0..num_nodes {
        let mut near_config = load_test_config(
            if i < num_validator_seats as usize { &validators[i] } else { "" },
            if i == 0 { first_node } else { open_port() },
            genesis.clone(),
        );
        rpc_addrs.push(near_config.rpc_addr().unwrap().clone());
        near_config.client_config.min_num_peers = num_nodes - 1;
        if i > 0 {
            near_config.network_config.boot_nodes =
                convert_boot_nodes(vec![("near.0", first_node)]);
        }
        // if non validator, track all shards to track.
        if i >= (num_validator_seats as usize) && i < num_tracking_nodes {
            near_config.client_config.track_all_shards = true;
        }
        near_config.client_config.epoch_sync_enabled = false;
        near_configs.push(near_config);
    }

    let mut res = vec![];
    for (i, near_config) in near_configs.into_iter().enumerate() {
        let (client, view_client, arbiters) = start_with_config(dirs[i].path(), near_config);
        res.push((client, view_client, arbiters))
    }
    (genesis, rpc_addrs, res)
}
