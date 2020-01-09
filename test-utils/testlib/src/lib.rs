use std::sync::Arc;

use actix::Addr;
use tempdir::TempDir;

use near::{load_test_config, start_with_config, GenesisConfig, NightshadeRuntime};
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
use near_client::{ClientActor, ViewClientActor};
use near_network::test_utils::{convert_boot_nodes, open_port};
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::init_integration_logger;
use near_primitives::types::{BlockHeightDelta, NumSeats, NumShards, ShardId};
use near_store::test_utils::create_test_store;

pub mod actix_utils;
pub mod fees_utils;
pub mod node;
pub mod runtime_utils;
pub mod standard_test_cases;
pub mod test_helpers;
pub mod user;

/// Compute genesis hash from genesis config.
pub fn genesis_hash(genesis_config: &GenesisConfig) -> CryptoHash {
    genesis_header(&genesis_config).hash
}

/// Utility to generate genesis header from config for testing purposes.
pub fn genesis_header(genesis_config: &GenesisConfig) -> BlockHeader {
    let dir = TempDir::new("unused").unwrap();
    let store = create_test_store();
    let genesis_time = genesis_config.genesis_time.clone();
    let runtime = Arc::new(NightshadeRuntime::new(
        dir.path(),
        store.clone(),
        genesis_config.clone(),
        vec![],
        vec![],
    ));
    let chain_genesis = ChainGenesis::new(
        genesis_time,
        genesis_config.gas_limit,
        genesis_config.min_gas_price,
        genesis_config.total_supply,
        genesis_config.max_inflation_rate,
        genesis_config.gas_price_adjustment_rate,
        genesis_config.transaction_validity_period,
        genesis_config.epoch_length,
    );
    let chain =
        Chain::new(store, runtime, &chain_genesis, DoomslugThresholdMode::HalfStake).unwrap();
    chain.genesis().clone()
}

/// Utility to generate genesis header from config for testing purposes.
pub fn genesis_block(genesis_config: GenesisConfig) -> Block {
    let dir = TempDir::new("unused").unwrap();
    let store = create_test_store();
    let runtime = Arc::new(NightshadeRuntime::new(
        dir.path(),
        store.clone(),
        genesis_config.clone(),
        vec![],
        vec![],
    ));
    let mut chain =
        Chain::new(store, runtime, &genesis_config.into(), DoomslugThresholdMode::HalfStake)
            .unwrap();
    chain.get_block(&chain.genesis().hash()).unwrap().clone()
}

pub fn start_nodes(
    num_shards: NumShards,
    dirs: &[TempDir],
    num_validator_seats: NumSeats,
    num_lightclient: usize,
    epoch_length: BlockHeightDelta,
) -> (GenesisConfig, Vec<String>, Vec<(Addr<ClientActor>, Addr<ViewClientActor>)>) {
    init_integration_logger();

    let num_nodes = dirs.len();
    let num_tracking_nodes = dirs.len() - num_lightclient;
    let seeds = (0..num_nodes).map(|i| format!("near.{}", i)).collect::<Vec<_>>();
    let mut genesis_config = GenesisConfig::test_sharded(
        seeds.iter().map(|s| s.as_str()).collect(),
        num_validator_seats,
        (0..num_shards).map(|_| num_validator_seats).collect(),
    );
    genesis_config.epoch_length = epoch_length;

    let validators = (0..num_validator_seats).map(|i| format!("near.{}", i)).collect::<Vec<_>>();
    let mut near_configs = vec![];
    let first_node = open_port();
    let mut rpc_addrs = vec![];
    for i in 0..num_nodes {
        let mut near_config = load_test_config(
            if i < num_validator_seats as usize { &validators[i] } else { "" },
            if i == 0 { first_node } else { open_port() },
            &genesis_config,
        );
        rpc_addrs.push(near_config.rpc_config.addr.clone());
        near_config.client_config.min_num_peers = num_nodes - 1;
        if i > 0 {
            near_config.network_config.boot_nodes =
                convert_boot_nodes(vec![("near.0", first_node)]);
        }
        // if non validator, add some shards to track.
        if i >= (num_validator_seats as usize) && i < num_tracking_nodes {
            let shards_per_node =
                num_shards as usize / (num_tracking_nodes - num_validator_seats as usize);
            let (from, to) = (
                ((i - num_validator_seats as usize) * shards_per_node) as ShardId,
                ((i - (num_validator_seats as usize) + 1) * shards_per_node) as ShardId,
            );
            near_config.client_config.tracked_shards.extend(&(from..to).collect::<Vec<_>>());
        }
        near_configs.push(near_config);
    }

    let mut res = vec![];
    for (i, near_config) in near_configs.into_iter().enumerate() {
        let (client, view_client) = start_with_config(dirs[i].path(), near_config);
        res.push((client, view_client))
    }
    (genesis_config, rpc_addrs, res)
}
