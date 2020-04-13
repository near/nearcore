use std::fs::File;
use std::path::Path;

use near_chain_configs::{Genesis, GenesisConfig, GENESIS_CONFIG_VERSION, PROTOCOL_VERSION};
use near_primitives::types::{Balance, NumShards, ShardId};
use near_primitives::utils::get_num_seats_per_shard;
use neard::config::{
    Config, BLOCK_PRODUCER_KICKOUT_THRESHOLD, CHUNK_PRODUCER_KICKOUT_THRESHOLD, CONFIG_FILENAME,
    DEVELOPER_PERCENT, EXPECTED_EPOCH_LENGTH, FISHERMEN_THRESHOLD, GAS_PRICE_ADJUSTMENT_RATE,
    GENESIS_CONFIG_FILENAME, INITIAL_GAS_LIMIT, MAX_INFLATION_RATE, MIN_GAS_PRICE, NODE_KEY_FILE,
    NUM_BLOCKS_PER_YEAR, NUM_BLOCK_PRODUCER_SEATS, PROTOCOL_PERCENT, TRANSACTION_VALIDITY_PERIOD,
};
use neard::NEAR_BASE;

const ACCOUNTS_FILE: &str = "accounts.csv";
const NUM_SHARDS: NumShards = 8;

fn verify_total_supply(total_supply: Balance, chain_id: &String) {
    if chain_id == "mainnet" {
        assert_eq!(
            total_supply,
            1_000_000_000 * NEAR_BASE,
            "Total supply should be exactly 1 billion"
        );
    } else if total_supply > 10_000_000_000 * NEAR_BASE
        && (chain_id == "testnet" || chain_id == "stakewars")
    {
        panic!("Total supply should not be more than 10 billion");
    }
}

/// Generates `config.json` and `genesis.config` from csv files.
/// Verifies that `validator_key.json`, and `node_key.json` are present.
pub fn csv_to_json_configs(home: &Path, chain_id: String, tracked_shards: Vec<ShardId>) {
    // Verify that key files exist.
    assert!(home.join(NODE_KEY_FILE).as_path().exists(), "Node key file should exist");

    if tracked_shards.iter().any(|shard_id| *shard_id >= NUM_SHARDS) {
        panic!("Trying to track a shard that does not exist");
    }

    // Construct `config.json`.
    let mut config = Config::default();
    config.tracked_shards = tracked_shards;

    // Construct genesis config.
    let (records, validators, peer_info, treasury, genesis_time) =
        crate::csv_parser::keys_to_state_records(
            File::open(home.join(ACCOUNTS_FILE)).expect("Error opening accounts file."),
            MIN_GAS_PRICE,
        )
        .expect("Error parsing accounts file.");
    config.network.boot_nodes =
        peer_info.into_iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",");
    let genesis_config = GenesisConfig {
        protocol_version: PROTOCOL_VERSION,
        config_version: GENESIS_CONFIG_VERSION,
        genesis_time,
        chain_id: chain_id.clone(),
        num_block_producer_seats: NUM_BLOCK_PRODUCER_SEATS,
        num_block_producer_seats_per_shard: get_num_seats_per_shard(
            NUM_SHARDS,
            NUM_BLOCK_PRODUCER_SEATS,
        ),
        avg_hidden_validator_seats_per_shard: vec![0; NUM_SHARDS as usize],
        dynamic_resharding: false,
        epoch_length: EXPECTED_EPOCH_LENGTH,
        gas_limit: INITIAL_GAS_LIMIT,
        gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
        block_producer_kickout_threshold: BLOCK_PRODUCER_KICKOUT_THRESHOLD,
        validators,
        transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
        developer_reward_percentage: DEVELOPER_PERCENT,
        protocol_reward_percentage: PROTOCOL_PERCENT,
        max_inflation_rate: MAX_INFLATION_RATE,
        num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
        protocol_treasury_account: treasury,
        chunk_producer_kickout_threshold: CHUNK_PRODUCER_KICKOUT_THRESHOLD,
        min_gas_price: MIN_GAS_PRICE,
        fishermen_threshold: FISHERMEN_THRESHOLD,
        ..Default::default()
    };
    let genesis = Genesis::new(genesis_config, records.into());
    verify_total_supply(genesis.config.total_supply, &chain_id);

    // Write all configs to files.
    config.write_to_file(&home.join(CONFIG_FILENAME));
    genesis.to_file(&home.join(GENESIS_CONFIG_FILENAME));
}
