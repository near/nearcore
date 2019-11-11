use std::fs::File;
use std::path::Path;

use near::config::{
    get_initial_supply, Config, BLOCK_PRODUCER_KICKOUT_THRESHOLD, CHUNK_PRODUCER_KICKOUT_THRESHOLD,
    CONFIG_FILENAME, DEVELOPER_PERCENT, EXPECTED_EPOCH_LENGTH, GAS_PRICE_ADJUSTMENT_RATE,
    GENESIS_CONFIG_FILENAME, INITIAL_GAS_LIMIT, INITIAL_GAS_PRICE, MAX_INFLATION_RATE,
    NODE_KEY_FILE, NUM_BLOCKS_PER_YEAR, NUM_BLOCK_PRODUCERS, PROTOCOL_PERCENT,
    TRANSACTION_VALIDITY_PERIOD, VALIDATOR_KEY_FILE,
};
use near::{GenesisConfig, NEAR_BASE};
use near_network::types::PROTOCOL_VERSION;
use near_primitives::types::Balance;
use near_primitives::types::ShardId;
use near_primitives::utils::get_num_block_producers_per_shard;

const ACCOUNTS_FILE: &str = "accounts.csv";
const NUM_SHARDS: usize = 8;

fn verify_total_supply(total_supply: Balance, chain_id: &String) {
    if chain_id == "mainnet" {
        assert_eq!(total_supply, 1_000_000_000, "Total supply should be exactly 1 billion");
    } else if total_supply > 10_000_000_000 * NEAR_BASE
        && (chain_id == "testnet" || chain_id == "stakewars")
    {
        panic!("Total supply should not be more than 10 billion");
    }
}

/// Generates `config.json` and `genesis.config` from csv files.
/// Verifies that `validator_key.json`, and `node_key.json` are present.
pub fn csv_to_json_configs(home: &Path, chain_id: String) {
    // Verify that key files exist.
    assert!(home.join(VALIDATOR_KEY_FILE).as_path().exists(), "Validator key file should exist");
    assert!(home.join(NODE_KEY_FILE).as_path().exists(), "Node key file should exist");

    // Construct `config.json`.
    let mut config = Config::default();
    config.telemetry.endpoints.push(near::config::DEFAULT_TELEMETRY_URL.to_string());

    // Construct genesis config.
    let (records, validators, peer_info, treasury, genesis_time) =
        crate::csv_parser::keys_to_state_records(
            File::open(home.join(ACCOUNTS_FILE)).expect("Error opening accounts file."),
            INITIAL_GAS_PRICE,
        )
        .expect("Error parsing accounts file.");
    config.network.boot_nodes =
        peer_info.into_iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",");
    let total_supply = get_initial_supply(&records);
    verify_total_supply(total_supply, &chain_id);
    let genesis_config = GenesisConfig {
        protocol_version: PROTOCOL_VERSION,
        genesis_time,
        chain_id,
        num_block_producers: 50,
        block_producers_per_shard: get_num_block_producers_per_shard(
            NUM_SHARDS as ShardId,
            NUM_BLOCK_PRODUCERS,
        ),
        avg_fisherman_per_shard: (0..NUM_SHARDS).map(|_| 0).collect(),
        dynamic_resharding: false,
        epoch_length: EXPECTED_EPOCH_LENGTH,
        gas_limit: INITIAL_GAS_LIMIT,
        gas_price: INITIAL_GAS_PRICE,
        gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
        block_producer_kickout_threshold: BLOCK_PRODUCER_KICKOUT_THRESHOLD,
        runtime_config: Default::default(),
        validators,
        transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
        records,
        developer_reward_percentage: DEVELOPER_PERCENT,
        protocol_reward_percentage: PROTOCOL_PERCENT,
        max_inflation_rate: MAX_INFLATION_RATE,
        total_supply,
        num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
        protocol_treasury_account: treasury,
        chunk_producer_kickout_threshold: CHUNK_PRODUCER_KICKOUT_THRESHOLD,
    };

    // Write all configs to files.
    config.write_to_file(&home.join(CONFIG_FILENAME));
    genesis_config.write_to_file(&home.join(GENESIS_CONFIG_FILENAME));
}
