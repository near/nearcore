use near_network::test_utils::open_port;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tracing::info;

use near_chain_configs::{Genesis, GenesisConfig};
use near_crypto::{InMemorySigner, KeyType, Signer};
#[cfg(feature = "json_rpc")]
use near_jsonrpc::RpcConfig;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::types::{AccountInfo, NumSeats, NumShards};
use near_primitives::utils::get_num_seats_per_shard;
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::version::PROTOCOL_VERSION;
#[cfg(feature = "rosetta_rpc")]
use near_rosetta_rpc::RosettaRpcConfig;
use nearcore::config::{
    add_account_with_key, add_protocol_account, random_chain_id, Config, NearConfig,
    BLOCK_PRODUCER_KICKOUT_THRESHOLD, CHUNK_PRODUCER_KICKOUT_THRESHOLD, CONFIG_FILENAME,
    FAST_EPOCH_LENGTH, FAST_MAX_BLOCK_PRODUCTION_DELAY, FAST_MIN_BLOCK_PRODUCTION_DELAY,
    FISHERMEN_THRESHOLD, GAS_PRICE_ADJUSTMENT_RATE, INITIAL_GAS_LIMIT, MAX_INFLATION_RATE,
    MIN_GAS_PRICE, NUM_BLOCKS_PER_YEAR, PROTOCOL_REWARD_RATE, PROTOCOL_TREASURY_ACCOUNT,
    PROTOCOL_UPGRADE_NUM_EPOCHS, PROTOCOL_UPGRADE_STAKE_THRESHOLD, TESTING_INIT_BALANCE,
    TESTING_INIT_STAKE, TRANSACTION_VALIDITY_PERIOD,
};

#[easy_ext::ext(GenesisExt)]
impl Genesis {
    pub fn test_with_seeds(
        seeds: Vec<&str>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
    ) -> Self {
        let mut validators = vec![];
        let mut records = vec![];
        for (i, &account) in seeds.iter().enumerate() {
            let signer = InMemorySigner::from_seed(account, KeyType::ED25519, account);
            let i = i as u64;
            if i < num_validator_seats {
                validators.push(AccountInfo {
                    account_id: account.to_string(),
                    public_key: signer.public_key.clone(),
                    amount: TESTING_INIT_STAKE,
                });
            }
            add_account_with_key(
                &mut records,
                account,
                &signer.public_key.clone(),
                TESTING_INIT_BALANCE - if i < num_validator_seats { TESTING_INIT_STAKE } else { 0 },
                if i < num_validator_seats { TESTING_INIT_STAKE } else { 0 },
                CryptoHash::default(),
            );
        }
        add_protocol_account(&mut records);
        let config = GenesisConfig {
            protocol_version: PROTOCOL_VERSION,
            genesis_time: Utc::now(),
            chain_id: random_chain_id(),
            num_block_producer_seats: num_validator_seats,
            num_block_producer_seats_per_shard: num_validator_seats_per_shard.clone(),
            avg_hidden_validator_seats_per_shard: vec![0; num_validator_seats_per_shard.len()],
            dynamic_resharding: false,
            protocol_upgrade_stake_threshold: PROTOCOL_UPGRADE_STAKE_THRESHOLD,
            protocol_upgrade_num_epochs: PROTOCOL_UPGRADE_NUM_EPOCHS,
            epoch_length: FAST_EPOCH_LENGTH,
            gas_limit: INITIAL_GAS_LIMIT,
            gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
            block_producer_kickout_threshold: BLOCK_PRODUCER_KICKOUT_THRESHOLD,
            validators,
            protocol_reward_rate: PROTOCOL_REWARD_RATE,
            max_inflation_rate: MAX_INFLATION_RATE,
            num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
            protocol_treasury_account: PROTOCOL_TREASURY_ACCOUNT.to_string(),
            transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
            chunk_producer_kickout_threshold: CHUNK_PRODUCER_KICKOUT_THRESHOLD,
            fishermen_threshold: FISHERMEN_THRESHOLD,
            min_gas_price: MIN_GAS_PRICE,
            ..Default::default()
        };
        Genesis::new(config, records.into())
    }

    pub fn test(seeds: Vec<&str>, num_validator_seats: NumSeats) -> Self {
        Self::test_with_seeds(seeds, num_validator_seats, vec![num_validator_seats])
    }

    pub fn test_free(seeds: Vec<&str>, num_validator_seats: NumSeats) -> Self {
        let mut genesis =
            Self::test_with_seeds(seeds, num_validator_seats, vec![num_validator_seats]);
        genesis.config.runtime_config = RuntimeConfig::free();
        genesis
    }

    pub fn test_sharded(
        seeds: Vec<&str>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
    ) -> Self {
        Self::test_with_seeds(seeds, num_validator_seats, num_validator_seats_per_shard)
    }
}

pub fn create_testnet_configs_from_seeds(
    seeds: Vec<String>,
    num_shards: NumShards,
    num_non_validator_seats: NumSeats,
    local_ports: bool,
    archive: bool,
) -> (Vec<Config>, Vec<InMemoryValidatorSigner>, Vec<InMemorySigner>, Genesis) {
    let num_validator_seats = (seeds.len() - num_non_validator_seats as usize) as NumSeats;
    let validator_signers = seeds
        .iter()
        .map(|seed| InMemoryValidatorSigner::from_seed(seed, KeyType::ED25519, seed))
        .collect::<Vec<_>>();
    let network_signers = seeds
        .iter()
        .map(|seed| InMemorySigner::from_seed("", KeyType::ED25519, seed))
        .collect::<Vec<_>>();
    let genesis = Genesis::test_sharded(
        seeds.iter().map(|s| s.as_str()).collect(),
        num_validator_seats,
        get_num_seats_per_shard(num_shards, num_validator_seats),
    );
    let mut configs = vec![];
    let first_node_port = open_port();
    for i in 0..seeds.len() {
        let mut config = Config::default();
        config.consensus.min_block_production_delay = Duration::from_millis(600);
        config.consensus.max_block_production_delay = Duration::from_millis(2000);
        if local_ports {
            config.network.addr =
                format!("127.0.0.1:{}", if i == 0 { first_node_port } else { open_port() });
            config.set_rpc_addr(format!("127.0.0.1:{}", open_port()));
            config.network.boot_nodes = if i == 0 {
                "".to_string()
            } else {
                format!("{}@127.0.0.1:{}", network_signers[0].public_key, first_node_port)
            };
            config.network.skip_sync_wait = num_validator_seats == 1;
        }
        config.archive = archive;
        config.consensus.min_num_peers =
            std::cmp::min(num_validator_seats as usize - 1, config.consensus.min_num_peers);
        configs.push(config);
    }
    (configs, validator_signers, network_signers, genesis)
}

/// Create testnet configuration. If `local_ports` is true,
/// sets up new ports for all nodes except the first one and sets boot node to it.
pub fn create_testnet_configs(
    num_shards: NumShards,
    num_validator_seats: NumSeats,
    num_non_validator_seats: NumSeats,
    prefix: &str,
    local_ports: bool,
    archive: bool,
) -> (Vec<Config>, Vec<InMemoryValidatorSigner>, Vec<InMemorySigner>, Genesis) {
    create_testnet_configs_from_seeds(
        (0..(num_validator_seats + num_non_validator_seats))
            .map(|i| format!("{}{}", prefix, i))
            .collect::<Vec<_>>(),
        num_shards,
        num_non_validator_seats,
        local_ports,
        archive,
    )
}

pub fn init_testnet_configs(
    dir: &Path,
    num_shards: NumShards,
    num_validator_seats: NumSeats,
    num_non_validator_seats: NumSeats,
    prefix: &str,
    archive: bool,
) {
    let (configs, validator_signers, network_signers, genesis) = create_testnet_configs(
        num_shards,
        num_validator_seats,
        num_non_validator_seats,
        prefix,
        false,
        archive,
    );
    for i in 0..(num_validator_seats + num_non_validator_seats) as usize {
        let node_dir = dir.join(format!("{}{}", prefix, i));
        fs::create_dir_all(node_dir.clone()).expect("Failed to create directory");

        validator_signers[i].write_to_file(&node_dir.join(&configs[i].validator_key_file));
        network_signers[i].write_to_file(&node_dir.join(&configs[i].node_key_file));

        genesis.to_file(&node_dir.join(&configs[i].genesis_file));
        configs[i].write_to_file(&node_dir.join(CONFIG_FILENAME));
        info!(target: "near", "Generated node key, validator key, genesis file in {}", node_dir.display());
    }
}

pub fn load_test_config(seed: &str, port: u16, genesis: Genesis) -> NearConfig {
    let mut config = Config::default();
    config.network.addr = format!("0.0.0.0:{}", port);
    config.set_rpc_addr(format!("0.0.0.0:{}", open_port()));
    config.consensus.min_block_production_delay =
        Duration::from_millis(FAST_MIN_BLOCK_PRODUCTION_DELAY);
    config.consensus.max_block_production_delay =
        Duration::from_millis(FAST_MAX_BLOCK_PRODUCTION_DELAY);
    let (signer, validator_signer) = if seed.is_empty() {
        let signer = Arc::new(InMemorySigner::from_random("".to_string(), KeyType::ED25519));
        (signer, None)
    } else {
        let signer = Arc::new(InMemorySigner::from_seed(seed, KeyType::ED25519, seed));
        let validator_signer =
            Arc::new(InMemoryValidatorSigner::from_seed(seed, KeyType::ED25519, seed))
                as Arc<dyn ValidatorSigner>;
        (signer, Some(validator_signer))
    };
    NearConfig::new(config, genesis, signer.into(), validator_signer)
}
