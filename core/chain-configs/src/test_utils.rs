use std::cmp::min;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use near_crypto::{InMemorySigner, PublicKey};
use near_primitives::account::{AccessKey, Account, AccountContract};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::types::{AccountId, AccountInfo, Balance, Gas, NumSeats, NumShards};
use near_primitives::utils::{from_timestamp, generate_random_string};
use near_primitives::version::PROTOCOL_VERSION;
use near_time::{Clock, Duration};
use num_rational::{Ratio, Rational32};

use crate::client_config::default_archival_writer_polling_interval;
use crate::{
    ClientConfig, CloudArchivalReaderConfig, CloudArchivalWriterConfig, CloudStorageConfig,
    EpochSyncConfig, ExternalStorageLocation, FAST_EPOCH_LENGTH, GAS_PRICE_ADJUSTMENT_RATE,
    GCConfig, Genesis, GenesisConfig, INITIAL_GAS_LIMIT, LogSummaryStyle, MAX_INFLATION_RATE,
    MIN_GAS_PRICE, MutableConfigValue, NUM_BLOCKS_PER_YEAR, PROTOCOL_REWARD_RATE,
    PROTOCOL_TREASURY_ACCOUNT, ReshardingConfig, StateSyncConfig, TRANSACTION_VALIDITY_PERIOD,
    TrackedShardsConfig, default_orphan_state_witness_max_size,
    default_orphan_state_witness_pool_size, default_produce_chunk_add_transactions_time_limit,
};

/// Returns the default value for the thread count associated with rpc-handler actor (currently
/// handling incoming transactions and chunk endorsement validations).
/// In the benchmarks no performance gains were observed when increasing the number of threads
/// above half of available cores.
pub fn default_rpc_handler_thread_count() -> usize {
    std::thread::available_parallelism().map(|v| v.get()).unwrap_or(16) / 2
}

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = Balance::from_near(1_000_000_000);

/// Validator's stake used in tests.
pub const TESTING_INIT_STAKE: Balance = Balance::from_near(50_000_000);

pub const TEST_STATE_SYNC_TIMEOUT: i64 = 5;

impl GenesisConfig {
    pub fn test(clock: Clock) -> Self {
        GenesisConfig {
            genesis_time: from_timestamp(clock.now_utc().unix_timestamp_nanos() as u64),
            genesis_height: 0,
            gas_limit: Gas::from_teragas(1000),
            min_gas_price: Balance::ZERO,
            max_gas_price: Balance::from_yoctonear(1_000_000_000),
            total_supply: Balance::from_yoctonear(1_000_000_000),
            gas_price_adjustment_rate: Ratio::from_integer(0),
            transaction_validity_period: 100,
            epoch_length: 5,
            protocol_version: PROTOCOL_VERSION,
            ..Default::default()
        }
    }
}

impl Genesis {
    // Creates new genesis with a given set of accounts and shard layout.
    // The first num_validator_seats from accounts will be treated as 'validators'.
    pub fn from_accounts(
        clock: Clock,
        accounts: Vec<AccountId>,
        num_validator_seats: NumSeats,
        shard_layout: ShardLayout,
    ) -> Self {
        let mut account_infos = vec![];
        for (i, account) in accounts.into_iter().enumerate() {
            let signer = InMemorySigner::test_signer(&account);
            account_infos.push(AccountInfo {
                account_id: account.clone(),
                public_key: signer.public_key(),
                amount: if i < num_validator_seats as usize {
                    TESTING_INIT_STAKE
                } else {
                    Balance::ZERO
                },
            });
        }
        let genesis_time = from_timestamp(clock.now_utc().unix_timestamp_nanos() as u64);
        Self::from_account_infos(genesis_time, account_infos, num_validator_seats, shard_layout)
    }

    // Creates new genesis with a given set of account infos and shard layout.
    // The first num_validator_seats from account_infos will be treated as 'validators'.
    pub fn from_account_infos(
        genesis_time: DateTime<Utc>,
        account_infos: Vec<AccountInfo>,
        num_validator_seats: NumSeats,
        shard_layout: ShardLayout,
    ) -> Self {
        let mut validators = vec![];
        let mut records = vec![];
        for (i, account_info) in account_infos.into_iter().enumerate() {
            if i < num_validator_seats as usize {
                validators.push(account_info.clone());
            }
            add_account_with_key(
                &mut records,
                account_info.account_id,
                &account_info.public_key,
                TESTING_INIT_BALANCE.checked_sub(account_info.amount).unwrap(),
                account_info.amount,
                CryptoHash::default(),
            );
        }
        add_protocol_account(&mut records);
        let epoch_config =
            Genesis::test_epoch_config(num_validator_seats, shard_layout, FAST_EPOCH_LENGTH);
        let config = GenesisConfig {
            protocol_version: PROTOCOL_VERSION,
            genesis_time,
            chain_id: random_chain_id(),
            dynamic_resharding: false,
            validators,
            protocol_reward_rate: PROTOCOL_REWARD_RATE,
            total_supply: get_initial_supply(&records),
            max_inflation_rate: MAX_INFLATION_RATE,
            num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
            protocol_treasury_account: PROTOCOL_TREASURY_ACCOUNT.parse().unwrap(),
            transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
            gas_limit: INITIAL_GAS_LIMIT,
            gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
            min_gas_price: MIN_GAS_PRICE,

            // epoch config parameters
            num_block_producer_seats: epoch_config.num_block_producer_seats,
            num_block_producer_seats_per_shard: epoch_config.num_block_producer_seats_per_shard,
            avg_hidden_validator_seats_per_shard: epoch_config.avg_hidden_validator_seats_per_shard,
            protocol_upgrade_stake_threshold: epoch_config.protocol_upgrade_stake_threshold,
            epoch_length: epoch_config.epoch_length,
            block_producer_kickout_threshold: epoch_config.block_producer_kickout_threshold,
            chunk_producer_kickout_threshold: epoch_config.chunk_producer_kickout_threshold,
            chunk_validator_only_kickout_threshold: epoch_config
                .chunk_validator_only_kickout_threshold,
            fishermen_threshold: epoch_config.fishermen_threshold,
            shard_layout: epoch_config.shard_layout,
            target_validator_mandates_per_shard: epoch_config.target_validator_mandates_per_shard,
            max_kickout_stake_perc: epoch_config.validator_max_kickout_stake_perc,
            online_min_threshold: epoch_config.online_min_threshold,
            online_max_threshold: epoch_config.online_max_threshold,
            minimum_stake_divisor: epoch_config.minimum_stake_divisor,
            num_chunk_producer_seats: epoch_config.num_chunk_producer_seats,
            num_chunk_validator_seats: epoch_config.num_chunk_validator_seats,
            minimum_validators_per_shard: epoch_config.minimum_validators_per_shard,
            minimum_stake_ratio: epoch_config.minimum_stake_ratio,
            chunk_producer_assignment_changes_limit: epoch_config
                .chunk_producer_assignment_changes_limit,
            shuffle_shard_assignment_for_chunk_producers: epoch_config
                .shuffle_shard_assignment_for_chunk_producers,

            ..Default::default()
        };
        Genesis::new(config, records.into()).unwrap()
    }

    pub fn test(accounts: Vec<AccountId>, num_validator_seats: NumSeats) -> Self {
        Self::from_accounts(
            Clock::real(),
            accounts,
            num_validator_seats,
            ShardLayout::single_shard(),
        )
    }

    pub fn test_sharded(
        clock: Clock,
        accounts: Vec<AccountId>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
    ) -> Self {
        let num_shards = num_validator_seats_per_shard.len() as NumShards;
        Self::from_accounts(
            clock,
            accounts,
            num_validator_seats,
            ShardLayout::multi_shard(num_shards, 0),
        )
    }

    pub fn test_sharded_new_version(
        accounts: Vec<AccountId>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
    ) -> Self {
        let num_shards = num_validator_seats_per_shard.len() as NumShards;
        Self::from_accounts(
            Clock::real(),
            accounts,
            num_validator_seats,
            ShardLayout::multi_shard(num_shards, 1),
        )
    }
}

pub fn add_protocol_account(records: &mut Vec<StateRecord>) {
    let signer = InMemorySigner::test_signer(&PROTOCOL_TREASURY_ACCOUNT.parse().unwrap());
    add_account_with_key(
        records,
        PROTOCOL_TREASURY_ACCOUNT.parse().unwrap(),
        &signer.public_key(),
        TESTING_INIT_BALANCE,
        Balance::ZERO,
        CryptoHash::default(),
    );
}

pub fn add_account_with_key(
    records: &mut Vec<StateRecord>,
    account_id: AccountId,
    public_key: &PublicKey,
    amount: Balance,
    staked: Balance,
    code_hash: CryptoHash,
) {
    records.push(StateRecord::Account {
        account_id: account_id.clone(),
        account: Account::new(amount, staked, AccountContract::from_local_code_hash(code_hash), 0),
    });
    records.push(StateRecord::AccessKey {
        account_id,
        public_key: public_key.clone(),
        access_key: AccessKey::full_access(),
    });
}

pub fn random_chain_id() -> String {
    format!("test-chain-{}", generate_random_string(5))
}

pub fn get_initial_supply(records: &[StateRecord]) -> Balance {
    let mut total_supply = Balance::ZERO;
    for record in records {
        if let StateRecord::Account { account, .. } = record {
            total_supply = total_supply
                .checked_add(account.amount().checked_add(account.locked()).unwrap())
                .unwrap();
        }
    }
    total_supply
}

pub fn test_cloud_archival_configs(
    cloud_archival_dir: impl Into<PathBuf>,
) -> (CloudArchivalReaderConfig, CloudArchivalWriterConfig) {
    let cloud_storage = CloudStorageConfig {
        storage: ExternalStorageLocation::Filesystem { root_dir: cloud_archival_dir.into() },
        credentials_file: None,
    };
    let reader_config = CloudArchivalReaderConfig { cloud_storage: cloud_storage.clone() };
    let writer_config = CloudArchivalWriterConfig {
        cloud_storage,
        archive_block_data: true,
        polling_interval: default_archival_writer_polling_interval(),
    };
    (reader_config, writer_config)
}

/// Common parameters used to set up test client.
pub struct TestClientConfigParams {
    pub skip_sync_wait: bool,
    pub min_block_prod_time: u64,
    pub max_block_prod_time: u64,
    pub num_block_producer_seats: NumSeats,
    pub enable_split_store: bool,
    pub enable_cloud_archival_writer: bool,
    pub save_trie_changes: bool,
    pub state_sync_enabled: bool,
}

impl ClientConfig {
    pub fn test(params: TestClientConfigParams) -> ClientConfig {
        let TestClientConfigParams {
            skip_sync_wait,
            min_block_prod_time,
            max_block_prod_time,
            num_block_producer_seats,
            enable_split_store,
            enable_cloud_archival_writer,
            save_trie_changes,
            state_sync_enabled,
        } = params;

        // TODO(cloud_archival) Revisit for cloud archival reader
        let archive = enable_split_store || enable_cloud_archival_writer;
        assert!(
            archive || save_trie_changes,
            "Configuration with archive = false and save_trie_changes = false is not supported \
            because non-archival nodes must save trie changes in order to do garbage collection."
        );
        let cloud_archival_writer = if enable_cloud_archival_writer {
            let (_, writer_config) = test_cloud_archival_configs("");
            Some(writer_config)
        } else {
            None
        };

        ClientConfig {
            version: Default::default(),
            chain_id: "unittest".to_string(),
            rpc_addr: Some("0.0.0.0:3030".to_string()),
            expected_shutdown: MutableConfigValue::new(None, "expected_shutdown"),
            block_production_tracking_delay: Duration::milliseconds(std::cmp::max(
                10,
                min_block_prod_time / 5,
            ) as i64),
            min_block_production_delay: Duration::milliseconds(min_block_prod_time as i64),
            max_block_production_delay: Duration::milliseconds(max_block_prod_time as i64),
            max_block_wait_delay: Duration::milliseconds(3 * min_block_prod_time as i64),
            chunk_wait_mult: Rational32::new(1, 6),
            skip_sync_wait,
            sync_check_period: Duration::milliseconds(100),
            sync_step_period: Duration::milliseconds(10),
            sync_height_threshold: 1,
            sync_max_block_requests: 10,
            header_sync_initial_timeout: Duration::seconds(10),
            header_sync_progress_timeout: Duration::seconds(2),
            header_sync_stall_ban_timeout: Duration::seconds(30),
            state_sync_external_timeout: Duration::seconds(TEST_STATE_SYNC_TIMEOUT),
            state_sync_p2p_timeout: Duration::seconds(TEST_STATE_SYNC_TIMEOUT),
            state_sync_retry_backoff: Duration::seconds(TEST_STATE_SYNC_TIMEOUT),
            state_sync_external_backoff: Duration::seconds(TEST_STATE_SYNC_TIMEOUT),
            header_sync_expected_height_per_second: 1,
            min_num_peers: 1,
            log_summary_period: Duration::seconds(10),
            produce_empty_blocks: true,
            epoch_length: 10,
            num_block_producer_seats,
            ttl_account_id_router: Duration::seconds(60 * 60),
            block_fetch_horizon: 50,
            catchup_step_period: Duration::milliseconds(100),
            chunk_request_retry_period: min(
                Duration::milliseconds(100),
                Duration::milliseconds(min_block_prod_time as i64 / 5),
            ),
            doomslug_step_period: Duration::milliseconds(100),
            block_header_fetch_horizon: 50,
            gc: GCConfig { gc_blocks_limit: 100, ..GCConfig::default() },
            tracked_shards_config: TrackedShardsConfig::NoShards,
            archive,
            cloud_archival_reader: None,
            cloud_archival_writer,
            save_trie_changes,
            save_untracked_partial_chunks_parts: true,
            save_tx_outcomes: true,
            log_summary_style: LogSummaryStyle::Colored,
            view_client_threads: 1,
            chunk_validation_threads: 1,
            state_request_throttle_period: Duration::seconds(1),
            state_requests_per_throttle_period: 30,
            state_request_server_threads: 1,
            trie_viewer_state_size_limit: None,
            max_gas_burnt_view: None,
            enable_statistics_export: true,
            client_background_migration_threads: 1,
            state_sync_enabled,
            state_sync: StateSyncConfig::default(),
            epoch_sync: EpochSyncConfig::default(),
            transaction_pool_size_limit: None,
            enable_multiline_logging: false,
            resharding_config: MutableConfigValue::new(
                ReshardingConfig::default(),
                "resharding_config",
            ),
            tx_routing_height_horizon: 4,
            produce_chunk_add_transactions_time_limit: MutableConfigValue::new(
                default_produce_chunk_add_transactions_time_limit(),
                "produce_chunk_add_transactions_time_limit",
            ),
            chunk_distribution_network: None,
            orphan_state_witness_pool_size: default_orphan_state_witness_pool_size(),
            orphan_state_witness_max_size: default_orphan_state_witness_max_size(),
            save_latest_witnesses: false,
            save_invalid_witnesses: false,
            transaction_request_handler_threads: default_rpc_handler_thread_count(),
            protocol_version_check: Default::default(),
        }
    }
}
