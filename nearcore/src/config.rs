use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use near_primitives::time::Clock;
use num_rational::Rational;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use tempfile::tempdir;
use tracing::{info, warn};

use near_chain_configs::{
    get_initial_supply, ClientConfig, GCConfig, Genesis, GenesisConfig, GenesisValidationMode,
    LogSummaryStyle,
};
use near_crypto::{InMemorySigner, KeyFile, KeyType, PublicKey, Signer};
#[cfg(feature = "json_rpc")]
use near_jsonrpc::RpcConfig;
use near_network::test_utils::open_port;
use near_network_primitives::types::{NetworkConfig, ValidatorConfig, ValidatorEndpoints, PeerInfo, ROUTED_MESSAGE_TTL};
use near_primitives::account::{AccessKey, Account};
use near_primitives::hash::CryptoHash;
#[cfg(test)]
use near_primitives::shard_layout::account_id_to_shard_id;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::types::{
    AccountId, AccountInfo, Balance, BlockHeightDelta, EpochHeight, Gas, NumBlocks, NumSeats,
    NumShards, ShardId,
};
use near_primitives::utils::{generate_random_string, get_num_seats_per_shard};
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::version::PROTOCOL_VERSION;
#[cfg(feature = "rosetta_rpc")]
use near_rosetta_rpc::RosettaRpcConfig;
use near_telemetry::TelemetryConfig;

use crate::download_file::{run_download_file, FileDownloadError};

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000 * NEAR_BASE;

/// Validator's stake used in tests.
pub const TESTING_INIT_STAKE: Balance = 50_000_000 * NEAR_BASE;

/// One NEAR, divisible by 10^24.
pub const NEAR_BASE: Balance = 1_000_000_000_000_000_000_000_000;

/// Millinear, 1/1000 of NEAR.
pub const MILLI_NEAR: Balance = NEAR_BASE / 1000;

/// Attonear, 1/10^18 of NEAR.
pub const ATTO_NEAR: Balance = 1;

/// Block production tracking delay.
pub const BLOCK_PRODUCTION_TRACKING_DELAY: u64 = 100;

/// Expected block production time in ms.
pub const MIN_BLOCK_PRODUCTION_DELAY: u64 = 600;

/// Maximum time to delay block production without approvals is ms.
pub const MAX_BLOCK_PRODUCTION_DELAY: u64 = 2_000;

/// Maximum time until skipping the previous block is ms.
pub const MAX_BLOCK_WAIT_DELAY: u64 = 6_000;

/// Reduce wait time for every missing block in ms.
const REDUCE_DELAY_FOR_MISSING_BLOCKS: u64 = 100;

/// Horizon at which instead of fetching block, fetch full state.
const BLOCK_FETCH_HORIZON: BlockHeightDelta = 50;

/// Horizon to step from the latest block when fetching state.
const STATE_FETCH_HORIZON: NumBlocks = 5;

/// Behind this horizon header fetch kicks in.
const BLOCK_HEADER_FETCH_HORIZON: BlockHeightDelta = 50;

/// Time between check to perform catchup.
const CATCHUP_STEP_PERIOD: u64 = 100;

/// Time between checking to re-request chunks.
const CHUNK_REQUEST_RETRY_PERIOD: u64 = 400;

/// Expected epoch length.
pub const EXPECTED_EPOCH_LENGTH: BlockHeightDelta = (5 * 60 * 1000) / MIN_BLOCK_PRODUCTION_DELAY;

/// Criterion for kicking out block producers.
pub const BLOCK_PRODUCER_KICKOUT_THRESHOLD: u8 = 90;

/// Criterion for kicking out chunk producers.
pub const CHUNK_PRODUCER_KICKOUT_THRESHOLD: u8 = 90;

/// Fast mode constants for testing/developing.
pub const FAST_MIN_BLOCK_PRODUCTION_DELAY: u64 = 120;
pub const FAST_MAX_BLOCK_PRODUCTION_DELAY: u64 = 500;
pub const FAST_EPOCH_LENGTH: BlockHeightDelta = 60;

/// Time to persist Accounts Id in the router without removing them in seconds.
pub const TTL_ACCOUNT_ID_ROUTER: u64 = 60 * 60;
/// Maximum amount of routes to store for each account id.
pub const MAX_ROUTES_TO_STORE: usize = 5;
/// Expected number of blocks per year
pub const NUM_BLOCKS_PER_YEAR: u64 = 365 * 24 * 60 * 60;

/// Initial gas limit.
pub const INITIAL_GAS_LIMIT: Gas = 1_000_000_000_000_000;

/// Initial gas price.
pub const MIN_GAS_PRICE: Balance = 1_000_000_000;

/// Protocol treasury account
pub const PROTOCOL_TREASURY_ACCOUNT: &str = "near";

/// Fishermen stake threshold.
pub const FISHERMEN_THRESHOLD: Balance = 10 * NEAR_BASE;

/// Number of blocks for which a given transaction is valid
pub const TRANSACTION_VALIDITY_PERIOD: NumBlocks = 100;

/// Number of seats for block producers
pub const NUM_BLOCK_PRODUCER_SEATS: NumSeats = 50;

/// How much height horizon to give to consider peer up to date.
pub const HIGHEST_PEER_HORIZON: u64 = 5;

/// The minimum stake required for staking is last seat price divided by this number.
pub const MINIMUM_STAKE_DIVISOR: u64 = 10;

/// Number of epochs before protocol upgrade.
pub const PROTOCOL_UPGRADE_NUM_EPOCHS: EpochHeight = 2;

pub const CONFIG_FILENAME: &str = "config.json";
pub const GENESIS_CONFIG_FILENAME: &str = "genesis.json";
pub const NODE_KEY_FILE: &str = "node_key.json";
pub const VALIDATOR_KEY_FILE: &str = "validator_key.json";

pub const MAINNET_TELEMETRY_URL: &str = "https://explorer.mainnet.near.org/api/nodes";
pub const NETWORK_TELEMETRY_URL: &str = "https://explorer.{}.near.org/api/nodes";

/// The rate at which the gas price can be adjusted (alpha in the formula).
/// The formula is
/// gas_price_t = gas_price_{t-1} * (1 + (gas_used/gas_limit - 1/2) * alpha))
pub const GAS_PRICE_ADJUSTMENT_RATE: Rational = Rational::new_raw(1, 100);

/// Protocol treasury reward
pub const PROTOCOL_REWARD_RATE: Rational = Rational::new_raw(1, 10);

/// Maximum inflation rate per year
pub const MAX_INFLATION_RATE: Rational = Rational::new_raw(1, 20);

/// Protocol upgrade stake threshold.
pub const PROTOCOL_UPGRADE_STAKE_THRESHOLD: Rational = Rational::new_raw(4, 5);

/// Maximum number of active peers. Hard limit.
fn default_max_num_peers() -> u32 {
    40
}
/// Minimum outbound connections a peer should have to avoid eclipse attacks.
fn default_minimum_outbound_connections() -> u32 {
    5
}
/// Lower bound of the ideal number of connections.
fn default_ideal_connections_lo() -> u32 {
    30
}
/// Upper bound of the ideal number of connections.
fn default_ideal_connections_hi() -> u32 {
    35
}
/// Peers which last message is was within this period of time are considered active recent peers.
fn default_peer_recent_time_window() -> Duration {
    Duration::from_secs(600)
}
/// Number of peers to keep while removing a connection.
/// Used to avoid disconnecting from peers we have been connected since long time.
fn default_safe_set_size() -> u32 {
    20
}
/// Lower bound of the number of connections to archival peers to keep
/// if we are an archival node.
fn default_archival_peer_connections_lower_bound() -> u32 {
    10
}
/// Time to persist Accounts Id in the router without removing them in seconds.
fn default_ttl_account_id_router() -> Duration {
    Duration::from_secs(TTL_ACCOUNT_ID_ROUTER)
}
/// Period to check on peer status
fn default_peer_stats_period() -> Duration {
    Duration::from_secs(5)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Network {
    /// Local address to listen for incoming connections.
    pub addr: String,
    /// Address to advertise to peers for them to connect.
    /// If empty, will use the same port as the addr, and will introspect on the listener.
    pub external_address: String,
    /// Comma separated list of nodes to connect to.
    pub boot_nodes: String,
    /// Comma separated list of whitelisted nodes. Inbound connections from the nodes on
    /// the whitelist are accepted even if the limit of the inbound connection has been reached.
    /// For each whitelisted node specifying both PeerId and IP:port is required:
    /// Example:
    ///   ed25519:86EtEy7epneKyrcJwSWP7zsisTkfDRH5CFVszt4qiQYw@31.192.22.209:24567
    #[serde(default)]
    pub whitelist_nodes: String,
    /// Maximum number of active peers. Hard limit.
    #[serde(default = "default_max_num_peers")]
    pub max_num_peers: u32,
    /// Minimum outbound connections a peer should have to avoid eclipse attacks.
    #[serde(default = "default_minimum_outbound_connections")]
    pub minimum_outbound_peers: u32,
    /// Lower bound of the ideal number of connections.
    #[serde(default = "default_ideal_connections_lo")]
    pub ideal_connections_lo: u32,
    /// Upper bound of the ideal number of connections.
    #[serde(default = "default_ideal_connections_hi")]
    pub ideal_connections_hi: u32,
    /// Peers which last message is was within this period of time are considered active recent peers (in seconds).
    #[serde(default = "default_peer_recent_time_window")]
    pub peer_recent_time_window: Duration,
    /// Number of peers to keep while removing a connection.
    /// Used to avoid disconnecting from peers we have been connected since long time.
    #[serde(default = "default_safe_set_size")]
    pub safe_set_size: u32,
    /// Lower bound of the number of connections to archival peers to keep
    /// if we are an archival node.
    #[serde(default = "default_archival_peer_connections_lower_bound")]
    pub archival_peer_connections_lower_bound: u32,
    /// Handshake timeout.
    pub handshake_timeout: Duration,
    /// Duration before trying to reconnect to a peer.
    pub reconnect_delay: Duration,
    /// Skip waiting for peers before starting node.
    pub skip_sync_wait: bool,
    /// Ban window for peers who misbehave.
    pub ban_window: Duration,
    /// List of addresses that will not be accepted as valid neighbors.
    /// It can be IP:Port or IP (to blacklist all connections coming from this address).
    #[serde(default)]
    pub blacklist: Vec<String>,
    /// Time to persist Accounts Id in the router without removing them in seconds.
    #[serde(default = "default_ttl_account_id_router")]
    pub ttl_account_id_router: Duration,
    /// Period to check on peer status
    #[serde(default = "default_peer_stats_period")]
    pub peer_stats_period: Duration,

    /// List of the public addresses (IP:port) of this node. Useful only if this node is a validator.
    /// This list will be signed and broadcasted to the whole network, so that everyone
    /// knows how to reach the validator.
    ///
    /// Recommended setup (requires static IP):
    /// In the simplest case this list should contains just 1 public address of this validator.
    /// In case the validator doesn't have a public IP (i.e. it is hidden in a private network),
    /// this list should contain public addresses of the trusted nodes which will be routing messages to the
    /// validator - validator will connect to these nodes immediately after startup.
    ///
    /// Less recommended setup (requires exactly one public dynamic/ephemeral or static IP):
    /// If the list is empty, the validator node will query trusted_stun_servers to determine its own IP.
    /// Only if the answer from the STUN servers is unambiguous (at least 1 server responds and
    /// all received responses provide the same IP), the IP (together with the port deduced from
    /// the addr field in this config) will be signed and broadcasted.
    /// 
    /// Discouraged setup (might be removed in the future)
    /// If the list is empty and STUN servers' response is ambiguous, the peers which connect to
    /// this validator node will natually observe the address of the validator and broadcast it.
    /// This setup is not reliable in presence of byzantine peers.
    #[serde(default)]
    pub public_addrs: Vec<String>,
    /// List of endpoints of trusted STUN servers (https://datatracker.ietf.org/doc/html/rfc8489).
    /// Used only if this node is a validator and public_ips is empty (see description of
    /// public_ips field). Format "<domain/ip>:<port>", for example "stun.l.google.com:19302".
    #[serde(default)] // TODO: add a default list.
    pub trusted_stun_servers: Vec<String>,
}

impl Default for Network {
    fn default() -> Self {
        Network {
            addr: "0.0.0.0:24567".to_string(),
            external_address: "".to_string(),
            boot_nodes: "".to_string(),
            whitelist_nodes: "".to_string(),
            max_num_peers: default_max_num_peers(),
            minimum_outbound_peers: default_minimum_outbound_connections(),
            ideal_connections_lo: default_ideal_connections_lo(),
            ideal_connections_hi: default_ideal_connections_hi(),
            peer_recent_time_window: default_peer_recent_time_window(),
            safe_set_size: default_safe_set_size(),
            archival_peer_connections_lower_bound: default_archival_peer_connections_lower_bound(),
            handshake_timeout: Duration::from_secs(20),
            reconnect_delay: Duration::from_secs(60),
            skip_sync_wait: false,
            ban_window: Duration::from_secs(3 * 60 * 60),
            blacklist: vec![],
            ttl_account_id_router: default_ttl_account_id_router(),
            peer_stats_period: default_peer_stats_period(),
            public_addrs: vec![],
            trusted_stun_servers: vec![],
        }
    }
}

/// Serde default only supports functions without parameters.
fn default_reduce_wait_for_missing_block() -> Duration {
    Duration::from_millis(REDUCE_DELAY_FOR_MISSING_BLOCKS)
}

fn default_header_sync_initial_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_header_sync_progress_timeout() -> Duration {
    Duration::from_secs(2)
}

fn default_header_sync_stall_ban_timeout() -> Duration {
    Duration::from_secs(120)
}

fn default_state_sync_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_header_sync_expected_height_per_second() -> u64 {
    10
}

fn default_sync_check_period() -> Duration {
    Duration::from_secs(10)
}

fn default_sync_step_period() -> Duration {
    Duration::from_millis(10)
}

fn default_view_client_threads() -> usize {
    4
}

fn default_doomslug_step_period() -> Duration {
    Duration::from_millis(100)
}

fn default_view_client_throttle_period() -> Duration {
    Duration::from_secs(30)
}

fn default_trie_viewer_state_size_limit() -> Option<u64> {
    Some(50_000)
}

fn default_use_checkpoints_for_db_migration() -> bool {
    true
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Consensus {
    /// Minimum number of peers to start syncing.
    pub min_num_peers: usize,
    /// Duration to check for producing / skipping block.
    pub block_production_tracking_delay: Duration,
    /// Minimum duration before producing block.
    pub min_block_production_delay: Duration,
    /// Maximum wait for approvals before producing block.
    pub max_block_production_delay: Duration,
    /// Maximum duration before skipping given height.
    pub max_block_wait_delay: Duration,
    /// Duration to reduce the wait for each missed block by validator.
    #[serde(default = "default_reduce_wait_for_missing_block")]
    pub reduce_wait_for_missing_block: Duration,
    /// Produce empty blocks, use `false` for testing.
    pub produce_empty_blocks: bool,
    /// Horizon at which instead of fetching block, fetch full state.
    pub block_fetch_horizon: BlockHeightDelta,
    /// Horizon to step from the latest block when fetching state.
    pub state_fetch_horizon: NumBlocks,
    /// Behind this horizon header fetch kicks in.
    pub block_header_fetch_horizon: BlockHeightDelta,
    /// Time between check to perform catchup.
    pub catchup_step_period: Duration,
    /// Time between checking to re-request chunks.
    pub chunk_request_retry_period: Duration,
    /// How much time to wait after initial header sync
    #[serde(default = "default_header_sync_initial_timeout")]
    pub header_sync_initial_timeout: Duration,
    /// How much time to wait after some progress is made in header sync
    #[serde(default = "default_header_sync_progress_timeout")]
    pub header_sync_progress_timeout: Duration,
    /// How much time to wait before banning a peer in header sync if sync is too slow
    #[serde(default = "default_header_sync_stall_ban_timeout")]
    pub header_sync_stall_ban_timeout: Duration,
    /// How much to wait for a state sync response before re-requesting
    #[serde(default = "default_state_sync_timeout")]
    pub state_sync_timeout: Duration,
    /// Expected increase of header head weight per second during header sync
    #[serde(default = "default_header_sync_expected_height_per_second")]
    pub header_sync_expected_height_per_second: u64,
    /// How frequently we check whether we need to sync
    #[serde(default = "default_sync_check_period")]
    pub sync_check_period: Duration,
    /// During sync the time we wait before reentering the sync loop
    #[serde(default = "default_sync_step_period")]
    pub sync_step_period: Duration,
    /// Time between running doomslug timer.
    #[serde(default = "default_doomslug_step_period")]
    pub doomslug_step_period: Duration,
}

impl Default for Consensus {
    fn default() -> Self {
        Consensus {
            min_num_peers: 3,
            block_production_tracking_delay: Duration::from_millis(BLOCK_PRODUCTION_TRACKING_DELAY),
            min_block_production_delay: Duration::from_millis(MIN_BLOCK_PRODUCTION_DELAY),
            max_block_production_delay: Duration::from_millis(MAX_BLOCK_PRODUCTION_DELAY),
            max_block_wait_delay: Duration::from_millis(MAX_BLOCK_WAIT_DELAY),
            reduce_wait_for_missing_block: default_reduce_wait_for_missing_block(),
            produce_empty_blocks: true,
            block_fetch_horizon: BLOCK_FETCH_HORIZON,
            state_fetch_horizon: STATE_FETCH_HORIZON,
            block_header_fetch_horizon: BLOCK_HEADER_FETCH_HORIZON,
            catchup_step_period: Duration::from_millis(CATCHUP_STEP_PERIOD),
            chunk_request_retry_period: Duration::from_millis(CHUNK_REQUEST_RETRY_PERIOD),
            header_sync_initial_timeout: default_header_sync_initial_timeout(),
            header_sync_progress_timeout: default_header_sync_progress_timeout(),
            header_sync_stall_ban_timeout: default_header_sync_stall_ban_timeout(),
            state_sync_timeout: default_state_sync_timeout(),
            header_sync_expected_height_per_second: default_header_sync_expected_height_per_second(
            ),
            sync_check_period: default_sync_check_period(),
            sync_step_period: default_sync_step_period(),
            doomslug_step_period: default_doomslug_step_period(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Config {
    pub genesis_file: String,
    pub genesis_records_file: Option<String>,
    pub validator_key_file: String,
    pub node_key_file: String,
    #[cfg(feature = "json_rpc")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc: Option<RpcConfig>,
    #[cfg(feature = "rosetta_rpc")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rosetta_rpc: Option<RosettaRpcConfig>,
    pub telemetry: TelemetryConfig,
    pub network: Network,
    pub consensus: Consensus,
    pub tracked_accounts: Vec<AccountId>,
    pub tracked_shards: Vec<ShardId>,
    pub archive: bool,
    pub log_summary_style: LogSummaryStyle,
    /// Garbage collection configuration.
    #[serde(default, flatten)]
    pub gc: GCConfig,
    #[serde(default = "default_view_client_threads")]
    pub view_client_threads: usize,
    pub epoch_sync_enabled: bool,
    #[serde(default = "default_view_client_throttle_period")]
    pub view_client_throttle_period: Duration,
    #[serde(default = "default_trie_viewer_state_size_limit")]
    pub trie_viewer_state_size_limit: Option<u64>,
    /// If set, overrides value in genesis configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_gas_burnt_view: Option<Gas>,
    /// Checkpoints let the user recover from interrupted DB migrations.
    #[serde(default = "default_use_checkpoints_for_db_migration")]
    pub use_db_migration_snapshot: bool,
    /// Location of the DB checkpoint for the DB migrations. This can be one of the following:
    /// * Empty, the checkpoint will be created in the database location, i.e. '$home/data'.
    /// * Absolute path that points to an existing directory. The checkpoint will be a sub-directory in that directory.
    /// For example, setting "use_db_migration_snapshot" to "/tmp/" will create a directory "/tmp/db_migration_snapshot" and populate it with the database files.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_migration_snapshot_path: Option<PathBuf>,
    /// Different parameters to configure/optimize underlying storage.
    pub store: near_store::StoreConfig,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            genesis_file: GENESIS_CONFIG_FILENAME.to_string(),
            genesis_records_file: None,
            validator_key_file: VALIDATOR_KEY_FILE.to_string(),
            node_key_file: NODE_KEY_FILE.to_string(),
            #[cfg(feature = "json_rpc")]
            rpc: Some(RpcConfig::default()),
            #[cfg(feature = "rosetta_rpc")]
            rosetta_rpc: None,
            telemetry: TelemetryConfig::default(),
            network: Network::default(),
            consensus: Consensus::default(),
            tracked_accounts: vec![],
            tracked_shards: vec![],
            archive: false,
            log_summary_style: LogSummaryStyle::Colored,
            gc: GCConfig::default(),
            epoch_sync_enabled: true,
            view_client_threads: default_view_client_threads(),
            view_client_throttle_period: default_view_client_throttle_period(),
            trie_viewer_state_size_limit: default_trie_viewer_state_size_limit(),
            max_gas_burnt_view: None,
            db_migration_snapshot_path: None,
            use_db_migration_snapshot: true,
            store: near_store::StoreConfig::default(),
        }
    }
}

impl Config {
    pub fn from_file(path: &Path) -> anyhow::Result<Self> {
        let mut unrecognised_fields = Vec::new();
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config from {}", path.display()))?;
        let config =
            serde_ignored::deserialize(&mut serde_json::Deserializer::from_str(&s), |path| {
                unrecognised_fields.push(path.to_string());
            })
            .with_context(|| format!("Failed to deserialize config from {}", path.display()))?;

        if !unrecognised_fields.is_empty() {
            warn!("{}: encountered unrecognised fields: {:?}", path.display(), unrecognised_fields);
        }

        Ok(config)
    }

    pub fn write_to_file(&self, path: &Path) -> std::io::Result<()> {
        let mut file = File::create(path)?;
        let str = serde_json::to_string_pretty(self)?;
        file.write_all(str.as_bytes())
    }

    pub fn rpc_addr(&self) -> Option<&str> {
        #[cfg(feature = "json_rpc")]
        if let Some(rpc) = &self.rpc {
            return Some(&rpc.addr);
        }
        None
    }

    #[allow(unused_variables)]
    pub fn set_rpc_addr(&mut self, addr: String) {
        #[cfg(feature = "json_rpc")]
        {
            self.rpc.get_or_insert(Default::default()).addr = addr;
        }
    }
}

#[easy_ext::ext(GenesisExt)]
impl Genesis {
    // Creates new genesis with a given set of accounts and shard layout.
    // The first num_validator_seats from accounts will be treated as 'validators'.
    pub fn test_with_seeds(
        accounts: Vec<AccountId>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
        shard_layout: ShardLayout,
    ) -> Self {
        let mut validators = vec![];
        let mut records = vec![];
        for (i, account) in accounts.into_iter().enumerate() {
            let signer =
                InMemorySigner::from_seed(account.clone(), KeyType::ED25519, account.as_ref());
            let i = i as u64;
            if i < num_validator_seats {
                validators.push(AccountInfo {
                    account_id: account.clone(),
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
            genesis_time: Clock::utc(),
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
            total_supply: get_initial_supply(&records),
            max_inflation_rate: MAX_INFLATION_RATE,
            num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
            protocol_treasury_account: PROTOCOL_TREASURY_ACCOUNT.parse().unwrap(),
            transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
            chunk_producer_kickout_threshold: CHUNK_PRODUCER_KICKOUT_THRESHOLD,
            fishermen_threshold: FISHERMEN_THRESHOLD,
            min_gas_price: MIN_GAS_PRICE,
            shard_layout,
            ..Default::default()
        };
        Genesis::new(config, records.into())
    }

    pub fn test(accounts: Vec<AccountId>, num_validator_seats: NumSeats) -> Self {
        Self::test_with_seeds(
            accounts,
            num_validator_seats,
            vec![num_validator_seats],
            ShardLayout::v0_single_shard(),
        )
    }

    pub fn test_sharded(
        accounts: Vec<AccountId>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
    ) -> Self {
        let num_shards = num_validator_seats_per_shard.len() as NumShards;
        Self::test_with_seeds(
            accounts,
            num_validator_seats,
            num_validator_seats_per_shard,
            ShardLayout::v0(num_shards, 0),
        )
    }

    pub fn test_sharded_new_version(
        accounts: Vec<AccountId>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
    ) -> Self {
        let num_shards = num_validator_seats_per_shard.len() as NumShards;
        Self::test_with_seeds(
            accounts,
            num_validator_seats,
            num_validator_seats_per_shard,
            ShardLayout::v0(num_shards, 1),
        )
    }
}

#[derive(Clone)]
pub struct NearConfig {
    pub config: Config,
    pub client_config: ClientConfig,
    pub network_config: NetworkConfig,
    #[cfg(feature = "json_rpc")]
    pub rpc_config: Option<RpcConfig>,
    #[cfg(feature = "rosetta_rpc")]
    pub rosetta_rpc_config: Option<RosettaRpcConfig>,
    pub telemetry_config: TelemetryConfig,
    pub genesis: Genesis,
    pub validator_signer: Option<Arc<dyn ValidatorSigner>>,
}

impl NearConfig {
    pub fn new(
        config: Config,
        genesis: Genesis,
        network_key_pair: KeyFile,
        validator_signer: Option<Arc<dyn ValidatorSigner>>,
    ) -> Self {
        NearConfig {
            config: config.clone(),
            client_config: ClientConfig {
                version: Default::default(),
                chain_id: genesis.config.chain_id.clone(),
                rpc_addr: config.rpc_addr().map(|addr| addr.to_owned()),
                block_production_tracking_delay: config.consensus.block_production_tracking_delay,
                min_block_production_delay: config.consensus.min_block_production_delay,
                max_block_production_delay: config.consensus.max_block_production_delay,
                max_block_wait_delay: config.consensus.max_block_wait_delay,
                reduce_wait_for_missing_block: config.consensus.reduce_wait_for_missing_block,
                skip_sync_wait: config.network.skip_sync_wait,
                sync_check_period: config.consensus.sync_check_period,
                sync_step_period: config.consensus.sync_step_period,
                sync_height_threshold: 1,
                header_sync_initial_timeout: config.consensus.header_sync_initial_timeout,
                header_sync_progress_timeout: config.consensus.header_sync_progress_timeout,
                header_sync_stall_ban_timeout: config.consensus.header_sync_stall_ban_timeout,
                header_sync_expected_height_per_second: config
                    .consensus
                    .header_sync_expected_height_per_second,
                state_sync_timeout: config.consensus.state_sync_timeout,
                min_num_peers: config.consensus.min_num_peers,
                log_summary_period: Duration::from_secs(10),
                produce_empty_blocks: config.consensus.produce_empty_blocks,
                epoch_length: genesis.config.epoch_length,
                num_block_producer_seats: genesis.config.num_block_producer_seats,
                announce_account_horizon: genesis.config.epoch_length / 2,
                ttl_account_id_router: config.network.ttl_account_id_router,
                // TODO(1047): this should be adjusted depending on the speed of sync of state.
                block_fetch_horizon: config.consensus.block_fetch_horizon,
                state_fetch_horizon: config.consensus.state_fetch_horizon,
                block_header_fetch_horizon: config.consensus.block_header_fetch_horizon,
                catchup_step_period: config.consensus.catchup_step_period,
                chunk_request_retry_period: config.consensus.chunk_request_retry_period,
                doosmslug_step_period: config.consensus.doomslug_step_period,
                tracked_accounts: config.tracked_accounts,
                tracked_shards: config.tracked_shards,
                archive: config.archive,
                log_summary_style: config.log_summary_style,
                gc: config.gc,
                view_client_threads: config.view_client_threads,
                epoch_sync_enabled: config.epoch_sync_enabled,
                view_client_throttle_period: config.view_client_throttle_period,
                trie_viewer_state_size_limit: config.trie_viewer_state_size_limit,
                max_gas_burnt_view: config.max_gas_burnt_view,
                enable_statistics_export: config.store.enable_statistics_export,
            },
            network_config: NetworkConfig {
                node_key: network_key_pair.secret_key,
                validator: validator_signer.as_ref().map(|signer| ValidatorConfig {
                    signer: signer.clone(),
                    endpoints: if config.network.public_addrs.len()>0 {
                        ValidatorEndpoints::PublicAddrs(
                            config.network.public_addrs.into_iter().map(|addr|
                                addr.parse().expect("Failed to parse SocketAddr")
                            ).collect(),
                        )
                    } else {
                       ValidatorEndpoints::TrustedStunServers(config.network.trusted_stun_servers)
                    },
                }),
                node_addr: match config.network.addr.as_str() {
                    "" => None,
                    addr => Some(addr.parse().expect("Failed to parse SocketAddr"))
                },
                boot_nodes: if config.network.boot_nodes.is_empty() {
                    vec![]
                } else {
                    config
                        .network
                        .boot_nodes
                        .split(',')
                        .map(|chunk| chunk.try_into().expect("Failed to parse PeerInfo"))
                        .collect()
                },
                whitelist_nodes: (|| -> Vec<_> {
                    let w = &config.network.whitelist_nodes;
                    if w.is_empty() {
                        return vec![];
                    }
                    let mut peers = vec![];
                    for peer in w.split(',') {
                        let peer: PeerInfo = peer.try_into().expect("Failed to parse PeerInfo");
                        if peer.addr.is_none() {
                            panic!(
                                "whitelist_nodes are required to specify both PeerId and IP:port"
                            )
                        }
                        peers.push(peer);
                    }
                    peers
                }()),
                handshake_timeout: config.network.handshake_timeout,
                reconnect_delay: config.network.reconnect_delay,
                bootstrap_peers_period: Duration::from_secs(60),
                max_num_peers: config.network.max_num_peers,
                minimum_outbound_peers: config.network.minimum_outbound_peers,
                ideal_connections_lo: config.network.ideal_connections_lo,
                ideal_connections_hi: config.network.ideal_connections_hi,
                peer_recent_time_window: config.network.peer_recent_time_window,
                safe_set_size: config.network.safe_set_size,
                archival_peer_connections_lower_bound: config
                    .network
                    .archival_peer_connections_lower_bound,
                ban_window: config.network.ban_window,
                max_send_peers: 512,
                peer_expiration_duration: Duration::from_secs(7 * 24 * 60 * 60),
                peer_stats_period: Duration::from_secs(5),
                ttl_account_id_router: config.network.ttl_account_id_router,
                routed_message_ttl: ROUTED_MESSAGE_TTL,
                max_routes_to_store: MAX_ROUTES_TO_STORE,
                highest_peer_horizon: HIGHEST_PEER_HORIZON,
                push_info_period: Duration::from_millis(100),
                blacklist: config.network.blacklist,
                outbound_disabled: false,
                archive: config.archive,
            },
            telemetry_config: config.telemetry,
            #[cfg(feature = "json_rpc")]
            rpc_config: config.rpc,
            #[cfg(feature = "rosetta_rpc")]
            rosetta_rpc_config: config.rosetta_rpc,
            genesis,
            validator_signer,
        }
    }

    pub fn rpc_addr(&self) -> Option<&str> {
        #[cfg(feature = "json_rpc")]
        if let Some(rpc) = &self.rpc_config {
            return Some(&rpc.addr);
        }
        None
    }
}

impl NearConfig {
    /// Test tool to save configs back to the folder.
    /// Useful for dynamic creating testnet configs and then saving them in different folders.
    pub fn save_to_dir(&self, dir: &Path) {
        fs::create_dir_all(dir).expect("Failed to create directory");

        self.config.write_to_file(&dir.join(CONFIG_FILENAME)).expect("Error writing config");

        if let Some(validator_signer) = &self.validator_signer {
            validator_signer
                .write_to_file(&dir.join(&self.config.validator_key_file))
                .expect("Error writing validator key file");
        }

        let network_signer = InMemorySigner::from_secret_key(
            "node".parse().unwrap(),
            self.network_config.node_key.clone(),
        );
        network_signer
            .write_to_file(&dir.join(&self.config.node_key_file))
            .expect("Error writing key file");

        self.genesis.to_file(&dir.join(&self.config.genesis_file));
    }
}

fn add_protocol_account(records: &mut Vec<StateRecord>) {
    let signer = InMemorySigner::from_seed(
        PROTOCOL_TREASURY_ACCOUNT.parse().unwrap(),
        KeyType::ED25519,
        PROTOCOL_TREASURY_ACCOUNT,
    );
    add_account_with_key(
        records,
        PROTOCOL_TREASURY_ACCOUNT.parse().unwrap(),
        &signer.public_key,
        TESTING_INIT_BALANCE,
        0,
        CryptoHash::default(),
    );
}

fn random_chain_id() -> String {
    format!("test-chain-{}", generate_random_string(5))
}

fn add_account_with_key(
    records: &mut Vec<StateRecord>,
    account_id: AccountId,
    public_key: &PublicKey,
    amount: u128,
    staked: u128,
    code_hash: CryptoHash,
) {
    records.push(StateRecord::Account {
        account_id: account_id.clone(),
        account: Account::new(amount, staked, code_hash, 0),
    });
    records.push(StateRecord::AccessKey {
        account_id,
        public_key: public_key.clone(),
        access_key: AccessKey::full_access(),
    });
}

/// Generates or loads a signer key from given file.
///
/// If the file already exists, loads the file (panicking if the file is
/// invalid), checks that account id in the file matches `account_id` if it’s
/// given and returns the key.  `test_seed` is ignored in this case.
///
/// If the file does not exist and `account_id` is not `None`, generates a new
/// key, saves it in the file and returns it.  If `test_seed` is not `None`, the
/// key generation algorithm is seeded with given string making it fully
/// deterministic.
fn generate_or_load_key(
    home_dir: &Path,
    filename: &str,
    account_id: Option<AccountId>,
    test_seed: Option<&str>,
) -> anyhow::Result<Option<InMemorySigner>> {
    let path = home_dir.join(filename);
    if path.exists() {
        let signer = InMemorySigner::from_file(&path)
            .with_context(|| format!("Failed initializing signer from {}", path.display()))?;
        if let Some(account_id) = account_id {
            if account_id != signer.account_id {
                return Err(anyhow!(
                    "‘{}’ contains key for {} but expecting key for {}",
                    path.display(),
                    signer.account_id,
                    account_id
                ));
            }
        }
        info!(target: "near", "Reusing key {} for {}", signer.public_key(), signer.account_id);
        Ok(Some(signer))
    } else if let Some(account_id) = account_id {
        let signer = if let Some(seed) = test_seed {
            InMemorySigner::from_seed(account_id, KeyType::ED25519, seed)
        } else {
            InMemorySigner::from_random(account_id, KeyType::ED25519)
        };
        info!(target: "near", "Using key {} for {}", signer.public_key(), signer.account_id);
        signer
            .write_to_file(&path)
            .with_context(|| anyhow!("Failed saving key to ‘{}’", path.display()))?;
        Ok(Some(signer))
    } else {
        Ok(None)
    }
}

#[test]
fn test_generate_or_load_key() {
    let tmp = tempfile::tempdir().unwrap();
    let home_dir = tmp.path();

    let gen = move |filename: &str, account: &str, seed: &str| {
        generate_or_load_key(
            home_dir,
            filename,
            if account.is_empty() { None } else { Some(account.parse().unwrap()) },
            if seed.is_empty() { None } else { Some(seed) },
        )
    };

    let test_ok = |filename: &str, account: &str, seed: &str| {
        let result = gen(filename, account, seed);
        let key = result.unwrap().unwrap();
        assert!(home_dir.join("key").exists());
        if !account.is_empty() {
            assert_eq!(account, key.account_id.as_str());
        }
        key
    };

    let test_err = |filename: &str, account: &str, seed: &str| {
        let result = gen(filename, account, seed);
        assert!(result.is_err());
    };

    // account_id == None → do nothing, return None
    assert!(generate_or_load_key(home_dir, "key", None, None).unwrap().is_none());
    assert!(!home_dir.join("key").exists());

    // account_id == Some, file doesn’t exist → create new key
    let key = test_ok("key", "fred", "");

    // file exists → load key, compare account if given
    assert!(key == test_ok("key", "", ""));
    assert!(key == test_ok("key", "fred", ""));
    test_err("key", "barney", "");

    // test_seed == Some → the same key is generated
    let k1 = test_ok("k1", "fred", "foo");
    let k2 = test_ok("k2", "barney", "foo");
    let k3 = test_ok("k3", "fred", "bar");

    assert!(k1.public_key == k2.public_key && k1.secret_key == k2.secret_key);
    assert!(k1 != k3);

    // file contains invalid JSON -> should return an error
    {
        let mut file = std::fs::File::create(&home_dir.join("bad_key")).unwrap();
        writeln!(file, "not JSON").unwrap();
    }
    test_err("bad_key", "fred", "");
}

pub fn mainnet_genesis() -> Genesis {
    lazy_static_include::lazy_static_include_bytes! {
        MAINNET_GENESIS_JSON => "res/mainnet_genesis.json",
    };
    serde_json::from_slice(*MAINNET_GENESIS_JSON).expect("Failed to deserialize mainnet genesis")
}

/// Initializes genesis and client configs and stores in the given folder
pub fn init_configs(
    dir: &Path,
    chain_id: Option<&str>,
    account_id: Option<AccountId>,
    test_seed: Option<&str>,
    num_shards: NumShards,
    fast: bool,
    genesis: Option<&str>,
    should_download_genesis: bool,
    download_genesis_url: Option<&str>,
    should_download_config: bool,
    download_config_url: Option<&str>,
    boot_nodes: Option<&str>,
    max_gas_burnt_view: Option<Gas>,
) -> anyhow::Result<()> {
    fs::create_dir_all(dir).with_context(|| anyhow!("Failed to create directory {:?}", dir))?;

    // Check if config already exists in home dir.
    if dir.join(CONFIG_FILENAME).exists() {
        let config = Config::from_file(&dir.join(CONFIG_FILENAME))
            .with_context(|| anyhow!("Failed to read config {}", dir.display()))?;
        let file_path = dir.join(&config.genesis_file);
        let genesis = GenesisConfig::from_file(&file_path).with_context(move || {
            anyhow!("Failed to read genesis config {}/{}", dir.display(), config.genesis_file)
        })?;
        bail!(
            "Config is already downloaded to ‘{}’ with chain-id ‘{}’.",
            file_path.display(),
            genesis.chain_id
        );
    }

    let mut config = Config::default();
    let chain_id = chain_id
        .and_then(|c| if c.is_empty() { None } else { Some(c.to_string()) })
        .unwrap_or_else(random_chain_id);

    if let Some(url) = download_config_url {
        download_config(&url.to_string(), &dir.join(CONFIG_FILENAME))
            .context(format!("Failed to download the config file from {}", url))?;
        config = Config::from_file(&dir.join(CONFIG_FILENAME))?;
    } else if should_download_config {
        let url = get_config_url(&chain_id);
        download_config(&url, &dir.join(CONFIG_FILENAME))
            .context(format!("Failed to download the config file from {}", url))?;
        config = Config::from_file(&dir.join(CONFIG_FILENAME))?;
    }

    if let Some(nodes) = boot_nodes {
        config.network.boot_nodes = nodes.to_string();
    }

    if max_gas_burnt_view.is_some() {
        config.max_gas_burnt_view = max_gas_burnt_view;
    }

    match chain_id.as_ref() {
        "mainnet" => {
            if test_seed.is_some() {
                bail!("Test seed is not supported for MainNet");
            }
            config.telemetry.endpoints.push(MAINNET_TELEMETRY_URL.to_string());
            config.write_to_file(&dir.join(CONFIG_FILENAME)).with_context(|| {
                format!("Error writing config to {}", dir.join(CONFIG_FILENAME).display())
            })?;

            let genesis = mainnet_genesis();

            generate_or_load_key(dir, &config.validator_key_file, account_id, None)?;
            generate_or_load_key(dir, &config.node_key_file, Some("node".parse().unwrap()), None)?;

            genesis.to_file(&dir.join(config.genesis_file));
            info!(target: "near", "Generated mainnet genesis file in {}", dir.display());
        }
        "testnet" | "betanet" => {
            if test_seed.is_some() {
                bail!("Test seed is not supported for official testnet");
            }
            config.telemetry.endpoints.push(NETWORK_TELEMETRY_URL.replace("{}", &chain_id));
            config.write_to_file(&dir.join(CONFIG_FILENAME)).with_context(|| {
                format!("Error writing config to {}", dir.join(CONFIG_FILENAME).display())
            })?;

            generate_or_load_key(dir, &config.validator_key_file, account_id, None)?;
            generate_or_load_key(dir, &config.node_key_file, Some("node".parse().unwrap()), None)?;

            // download genesis from s3
            let genesis_path = dir.join("genesis.json");
            let mut genesis_path_str =
                genesis_path.to_str().with_context(|| "Genesis path must be initialized")?;

            if let Some(url) = download_genesis_url {
                download_genesis(&url.to_string(), &genesis_path)
                    .context(format!("Failed to download the genesis file from {}", url))?;
            } else if should_download_genesis {
                let url = get_genesis_url(&chain_id);
                download_genesis(&url, &genesis_path)
                    .context(format!("Failed to download the genesis file from {}", url))?;
            } else {
                genesis_path_str = match genesis {
                    Some(g) => g,
                    None => {
                        bail!(
                            "Genesis file is required for {}.\
                             Use <--genesis|--download-genesis>",
                            &chain_id
                        );
                    }
                };
            }

            let mut genesis = Genesis::from_file(&genesis_path_str, GenesisValidationMode::Full);
            genesis.config.chain_id = chain_id.clone();

            genesis.to_file(&dir.join(config.genesis_file));
            info!(target: "near", "Generated for {} network node key and genesis file in {}", chain_id, dir.display());
        }
        _ => {
            // Create new configuration, key files and genesis for one validator.
            config.network.skip_sync_wait = true;
            if fast {
                config.consensus.min_block_production_delay =
                    Duration::from_millis(FAST_MIN_BLOCK_PRODUCTION_DELAY);
                config.consensus.max_block_production_delay =
                    Duration::from_millis(FAST_MAX_BLOCK_PRODUCTION_DELAY);
            }
            config.write_to_file(&dir.join(CONFIG_FILENAME)).with_context(|| {
                format!("Error writing config to {}", dir.join(CONFIG_FILENAME).display())
            })?;

            let account_id = account_id.unwrap_or_else(|| "test.near".parse().unwrap());
            let signer =
                generate_or_load_key(dir, &config.validator_key_file, Some(account_id), test_seed)?
                    .unwrap();
            generate_or_load_key(dir, &config.node_key_file, Some("node".parse().unwrap()), None)?;

            let mut records = vec![];
            add_account_with_key(
                &mut records,
                signer.account_id.clone(),
                &signer.public_key(),
                TESTING_INIT_BALANCE,
                TESTING_INIT_STAKE,
                CryptoHash::default(),
            );
            add_protocol_account(&mut records);
            let shards = if num_shards > 1 {
                ShardLayout::v1(
                    (0..num_shards - 1)
                        .map(|f| {
                            AccountId::from_str(format!("shard{}.test.near", f).as_str()).unwrap()
                        })
                        .collect(),
                    vec![],
                    None,
                    1,
                )
            } else {
                ShardLayout::v0_single_shard()
            };

            let genesis_config = GenesisConfig {
                protocol_version: PROTOCOL_VERSION,
                genesis_time: Clock::utc(),
                chain_id,
                genesis_height: 0,
                num_block_producer_seats: NUM_BLOCK_PRODUCER_SEATS,
                num_block_producer_seats_per_shard: get_num_seats_per_shard(
                    num_shards,
                    NUM_BLOCK_PRODUCER_SEATS,
                ),
                avg_hidden_validator_seats_per_shard: (0..num_shards).map(|_| 0).collect(),
                dynamic_resharding: false,
                protocol_upgrade_stake_threshold: PROTOCOL_UPGRADE_STAKE_THRESHOLD,
                protocol_upgrade_num_epochs: PROTOCOL_UPGRADE_NUM_EPOCHS,
                epoch_length: if fast { FAST_EPOCH_LENGTH } else { EXPECTED_EPOCH_LENGTH },
                gas_limit: INITIAL_GAS_LIMIT,
                gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
                block_producer_kickout_threshold: BLOCK_PRODUCER_KICKOUT_THRESHOLD,
                chunk_producer_kickout_threshold: CHUNK_PRODUCER_KICKOUT_THRESHOLD,
                online_max_threshold: Rational::new(99, 100),
                online_min_threshold: Rational::new(BLOCK_PRODUCER_KICKOUT_THRESHOLD as isize, 100),
                validators: vec![AccountInfo {
                    account_id: signer.account_id.clone(),
                    public_key: signer.public_key(),
                    amount: TESTING_INIT_STAKE,
                }],
                transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
                protocol_reward_rate: PROTOCOL_REWARD_RATE,
                max_inflation_rate: MAX_INFLATION_RATE,
                total_supply: get_initial_supply(&records),
                num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
                protocol_treasury_account: signer.account_id,
                fishermen_threshold: FISHERMEN_THRESHOLD,
                shard_layout: shards,
                min_gas_price: MIN_GAS_PRICE,
                ..Default::default()
            };
            let genesis = Genesis::new(genesis_config, records.into());
            genesis.to_file(&dir.join(config.genesis_file));
            info!(target: "near", "Generated node key, validator key, genesis file in {}", dir.display());
        }
    }
    Ok(())
}

pub fn create_testnet_configs_from_seeds(
    seeds: Vec<String>,
    num_shards: NumShards,
    num_non_validator_seats: NumSeats,
    local_ports: bool,
    archive: bool,
    fixed_shards: Option<Vec<String>>,
) -> (Vec<Config>, Vec<InMemoryValidatorSigner>, Vec<InMemorySigner>, Genesis) {
    let num_validator_seats = (seeds.len() - num_non_validator_seats as usize) as NumSeats;
    let validator_signers = seeds
        .iter()
        .map(|seed| {
            InMemoryValidatorSigner::from_seed(seed.parse().unwrap(), KeyType::ED25519, seed)
        })
        .collect::<Vec<_>>();
    let network_signers = seeds
        .iter()
        .map(|seed| InMemorySigner::from_seed("node".parse().unwrap(), KeyType::ED25519, seed))
        .collect::<Vec<_>>();

    let shard_layout = if let Some(ref fixed_shards) = fixed_shards {
        // If fixed shards are set, we expect that they take over all the shards (except for one that would host all the other accounts).
        assert!(fixed_shards.len() == num_shards as usize - 1);
        ShardLayout::v1(
            fixed_shards.iter().map(|it| it.parse().unwrap()).collect(),
            vec![],
            None,
            0,
        )
    } else {
        ShardLayout::v0(num_shards, 0)
    };
    let mut accounts_to_add_to_genesis: Vec<AccountId> =
        seeds.iter().map(|s| s.parse().unwrap()).collect();

    // If we have fixed shards - let's also add those accounts to genesis.
    if let Some(ref fixed_shards_accounts) = fixed_shards {
        accounts_to_add_to_genesis.extend(
            fixed_shards_accounts.iter().map(|s| s.parse().unwrap()).collect::<Vec<AccountId>>(),
        );
    };
    let genesis = Genesis::test_with_seeds(
        accounts_to_add_to_genesis,
        num_validator_seats,
        get_num_seats_per_shard(num_shards, num_validator_seats),
        shard_layout,
    );
    let mut configs = vec![];
    let first_node_port = open_port();
    for i in 0..seeds.len() {
        let mut config = Config::default();
        config.rpc.get_or_insert(Default::default()).enable_debug_rpc = true;
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
    fixed_shards: bool,
) -> (Vec<Config>, Vec<InMemoryValidatorSigner>, Vec<InMemorySigner>, Genesis, Vec<InMemorySigner>)
{
    let fixed_shards = if fixed_shards {
        Some((0..(num_shards - 1)).map(|i| format!("shard{}", i)).collect::<Vec<_>>())
    } else {
        None
    };
    let shard_keys = if let Some(ref fixed_shards) = fixed_shards {
        fixed_shards
            .iter()
            .map(|seed| InMemorySigner::from_seed(seed.parse().unwrap(), KeyType::ED25519, seed))
            .collect::<Vec<_>>()
    } else {
        vec![]
    };

    let (configs, validator_signers, network_signers, genesis) = create_testnet_configs_from_seeds(
        (0..(num_validator_seats + num_non_validator_seats))
            .map(|i| format!("{}{}", prefix, i))
            .collect::<Vec<_>>(),
        num_shards,
        num_non_validator_seats,
        local_ports,
        archive,
        fixed_shards,
    );

    (configs, validator_signers, network_signers, genesis, shard_keys)
}

pub fn init_testnet_configs(
    dir: &Path,
    num_shards: NumShards,
    num_validator_seats: NumSeats,
    num_non_validator_seats: NumSeats,
    prefix: &str,
    archive: bool,
    fixed_shards: bool,
) {
    let (configs, validator_signers, network_signers, genesis, shard_keys) = create_testnet_configs(
        num_shards,
        num_validator_seats,
        num_non_validator_seats,
        prefix,
        false,
        archive,
        fixed_shards,
    );
    for i in 0..(num_validator_seats + num_non_validator_seats) as usize {
        let node_dir = dir.join(format!("{}{}", prefix, i));
        fs::create_dir_all(node_dir.clone()).expect("Failed to create directory");

        validator_signers[i]
            .write_to_file(&node_dir.join(&configs[i].validator_key_file))
            .expect("Error writing validator key file");
        network_signers[i]
            .write_to_file(&node_dir.join(&configs[i].node_key_file))
            .expect("Error writing key file");
        for key in &shard_keys {
            key.write_to_file(&node_dir.join(format!("{}_key.json", key.account_id)))
                .expect("Error writing shard file");
        }

        genesis.to_file(&node_dir.join(&configs[i].genesis_file));
        configs[i].write_to_file(&node_dir.join(CONFIG_FILENAME)).expect("Error writing config");
        info!(target: "near", "Generated node key, validator key, genesis file in {}", node_dir.display());
    }
}

pub fn get_genesis_url(chain_id: &str) -> String {
    format!(
        "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/{}/genesis.json.xz",
        chain_id,
    )
}

pub fn get_config_url(chain_id: &str) -> String {
    format!(
        "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/{}/config.json",
        chain_id,
    )
}

pub fn download_genesis(url: &str, path: &Path) -> Result<(), FileDownloadError> {
    info!(target: "near", "Downloading genesis file from: {} ...", url);
    let result = run_download_file(url, path);
    if result.is_ok() {
        info!(target: "near", "Saved the genesis file to: {} ...", path.display());
    }
    result
}

pub fn download_config(url: &str, path: &Path) -> Result<(), FileDownloadError> {
    info!(target: "near", "Downloading config file from: {} ...", url);
    let result = run_download_file(url, path);
    if result.is_ok() {
        info!(target: "near", "Saved the config file to: {} ...", path.display());
    }
    result
}

#[derive(Deserialize)]
struct NodeKeyFile {
    account_id: String,
    public_key: PublicKey,
    secret_key: near_crypto::SecretKey,
}

impl NodeKeyFile {
    fn from_file(path: &Path) -> std::io::Result<Self> {
        let mut file = File::open(path)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        Ok(serde_json::from_str(&content)?)
    }
}

impl From<NodeKeyFile> for KeyFile {
    fn from(this: NodeKeyFile) -> Self {
        Self {
            account_id: if this.account_id.is_empty() {
                "node".to_string()
            } else {
                this.account_id
            }
            .try_into()
            .unwrap(),
            public_key: this.public_key,
            secret_key: this.secret_key,
        }
    }
}

pub fn load_config(
    dir: &Path,
    genesis_validation: GenesisValidationMode,
) -> Result<NearConfig, anyhow::Error> {
    let config = Config::from_file(&dir.join(CONFIG_FILENAME))?;
    let genesis_file = dir.join(&config.genesis_file);
    let validator_file = dir.join(&config.validator_key_file);
    let validator_signer = if validator_file.exists() {
        let signer = InMemoryValidatorSigner::from_file(&validator_file).with_context(|| {
            format!("Failed initializing validator signer from {}", validator_file.display())
        })?;
        Some(Arc::new(signer) as Arc<dyn ValidatorSigner>)
    } else {
        None
    };
    let node_key_path = dir.join(&config.node_key_file);
    let network_signer = NodeKeyFile::from_file(&node_key_path).with_context(|| {
        format!("Failed reading node key file from {}", node_key_path.display())
    })?;

    let genesis_records_file = config.genesis_records_file.clone();
    Ok(NearConfig::new(
        config,
        match genesis_records_file {
            Some(genesis_records_file) => Genesis::from_files(
                &genesis_file,
                &dir.join(genesis_records_file),
                genesis_validation,
            ),
            None => Genesis::from_file(&genesis_file, genesis_validation),
        },
        network_signer.into(),
        validator_signer,
    ))
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
        let signer =
            Arc::new(InMemorySigner::from_random("node".parse().unwrap(), KeyType::ED25519));
        (signer, None)
    } else {
        let signer =
            Arc::new(InMemorySigner::from_seed(seed.parse().unwrap(), KeyType::ED25519, seed));
        let validator_signer = Arc::new(InMemoryValidatorSigner::from_seed(
            seed.parse().unwrap(),
            KeyType::ED25519,
            seed,
        )) as Arc<dyn ValidatorSigner>;
        (signer, Some(validator_signer))
    };
    NearConfig::new(config, genesis, signer.into(), validator_signer)
}

#[test]
fn test_init_config_localnet() {
    // Check that we can initialize the config with multiple shards.
    let temp_dir = tempdir().unwrap();
    init_configs(
        &temp_dir.path(),
        Some("localnet"),
        None,
        Some("seed1"),
        3,
        false,
        None,
        false,
        None,
        false,
        None,
        None,
        None,
    )
    .unwrap();
    let genesis =
        Genesis::from_file(temp_dir.path().join("genesis.json"), GenesisValidationMode::UnsafeFast);
    assert_eq!(genesis.config.chain_id, "localnet");
    assert_eq!(genesis.config.shard_layout.num_shards(), 3);
    assert_eq!(
        account_id_to_shard_id(
            &AccountId::from_str("shard0.test.near").unwrap(),
            &genesis.config.shard_layout
        ),
        0
    );
    assert_eq!(
        account_id_to_shard_id(
            &AccountId::from_str("shard1.test.near").unwrap(),
            &genesis.config.shard_layout
        ),
        1
    );
    assert_eq!(
        account_id_to_shard_id(
            &AccountId::from_str("foobar.near").unwrap(),
            &genesis.config.shard_layout
        ),
        2
    );
}

/// Tests that loading a config.json file works and results in values being
/// correctly parsed and defaults being applied correctly applied.
#[test]
fn test_config_from_file() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR"));

    for (has_gc, path) in
        [(true, "res/example-config-gc.json"), (false, "res/example-config-no-gc.json")]
    {
        let path = base.join(path);
        let data = std::fs::read(path).unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.as_file().write_all(&data).unwrap();

        let config = Config::from_file(&tmp.into_temp_path()).unwrap();

        // TODO(mina86): We might want to add more checks.  Looking at all
        // values is probably not worth it but there may be some other defaults
        // we want to ensure that they happen.
        let want_gc = if has_gc {
            GCConfig { gc_blocks_limit: 42, gc_fork_clean_step: 420, gc_num_epochs_to_keep: 24 }
        } else {
            GCConfig { gc_blocks_limit: 2, gc_fork_clean_step: 100, gc_num_epochs_to_keep: 5 }
        };
        assert_eq!(want_gc, config.gc);

        assert_eq!(
            vec!["https://explorer.mainnet.near.org/api/nodes".to_string()],
            config.telemetry.endpoints
        );
    }
}
