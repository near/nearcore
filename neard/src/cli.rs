#[cfg(unix)]
use anyhow::Context;
use near_amend_genesis::AmendGenesisCommand;
use near_chain_configs::GenesisValidationMode;
use near_client::ConfigUpdater;
use near_cold_store_tool::ColdStoreCommand;
use near_config_utils::DownloadConfigType;
use near_database_tool::commands::DatabaseCommand;
use near_dyn_configs::{UpdateableConfigLoader, UpdateableConfigLoaderError, UpdateableConfigs};
use near_flat_storage::commands::FlatStorageCommand;
use near_fork_network::cli::ForkNetworkCommand;
use near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofResponse;
use near_mirror::MirrorCommand;
use near_network::tcp;
use near_o11y::tracing_subscriber::EnvFilter;
use near_o11y::{
    default_subscriber, default_subscriber_with_opentelemetry, BuildEnvFilterError,
    EnvFilterBuilder,
};
use near_ping::PingCommand;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::compute_root_from_path;
use near_primitives::types::{Gas, NumSeats, NumShards};
use near_replay_archive_tool::ReplayArchiveCommand;
use near_state_parts::cli::StatePartsCommand;
use near_state_parts_dump_check::cli::StatePartsDumpCheckCommand;
use near_state_viewer::StateViewerSubCommand;
use near_store::db::RocksDB;
use near_store::Mode;
use near_undo_block::cli::UndoBlockCommand;
use serde_json::Value;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tracing::{debug, error, info, warn};

/// NEAR Protocol Node
#[derive(clap::Parser)]
#[clap(version = crate::NEARD_VERSION_STRING.as_str())]
#[clap(subcommand_required = true, arg_required_else_help = true)]
pub(super) struct NeardCmd {
    #[clap(flatten)]
    opts: NeardOpts,
    #[clap(subcommand)]
    subcmd: NeardSubCommand,
}

impl NeardCmd {
    pub(super) fn parse_and_run() -> anyhow::Result<()> {
        let neard_cmd: Self = clap::Parser::parse();

        // Enable logging of the current thread.
        let _subscriber_guard = default_subscriber(
            make_env_filter(neard_cmd.opts.verbose_target())?,
            &neard_cmd.opts.o11y,
        )
        .local();

        info!(
            target: "neard",
            version = crate::NEARD_VERSION,
            build = crate::NEARD_BUILD,
            latest_protocol = near_primitives::version::PROTOCOL_VERSION
        );

        #[cfg(feature = "test_features")]
        {
            error!("THIS IS A NODE COMPILED WITH ADVERSARIAL BEHAVIORS. DO NOT USE IN PRODUCTION.");
            if std::env::var("ADVERSARY_CONSENT").unwrap_or_default() != "1" {
                error!(
                    "To run a node with adversarial behavior enabled give your consent \
                            by setting an environment variable:"
                );
                error!("ADVERSARY_CONSENT=1");
                std::process::exit(1);
            }
        }

        let home_dir = neard_cmd.opts.home.clone();
        let genesis_validation = if neard_cmd.opts.unsafe_fast_startup {
            GenesisValidationMode::UnsafeFast
        } else {
            GenesisValidationMode::Full
        };

        match neard_cmd.subcmd {
            NeardSubCommand::Init(cmd) => cmd.run(&home_dir)?,
            NeardSubCommand::Localnet(cmd) => cmd.run(&home_dir),
            NeardSubCommand::Run(cmd) => cmd.run(
                &home_dir,
                genesis_validation,
                neard_cmd.opts.verbose_target(),
                &neard_cmd.opts.o11y,
            ),

            NeardSubCommand::StateViewer(cmd) => {
                let mode = if cmd.readwrite { Mode::ReadWrite } else { Mode::ReadOnly };
                cmd.subcmd.run(&home_dir, genesis_validation, mode, cmd.store_temperature);
            }

            NeardSubCommand::VerifyProof(cmd) => {
                cmd.run();
            }
            NeardSubCommand::Ping(cmd) => {
                cmd.run()?;
            }
            NeardSubCommand::Mirror(cmd) => {
                cmd.run()?;
            }
            NeardSubCommand::AmendGenesis(cmd) => {
                cmd.run()?;
            }
            NeardSubCommand::ColdStore(cmd) => {
                cmd.run(&home_dir, genesis_validation)?;
            }
            NeardSubCommand::StateParts(cmd) => {
                cmd.run()?;
            }
            NeardSubCommand::FlatStorage(cmd) => {
                cmd.run(&home_dir, genesis_validation)?;
            }
            NeardSubCommand::ValidateConfig(cmd) => {
                cmd.run(&home_dir, genesis_validation)?;
            }
            NeardSubCommand::UndoBlock(cmd) => {
                cmd.run(&home_dir, genesis_validation)?;
            }
            NeardSubCommand::Database(cmd) => {
                cmd.run(&home_dir, genesis_validation)?;
            }
            NeardSubCommand::ForkNetwork(cmd) => {
                cmd.run(
                    &home_dir,
                    genesis_validation,
                    neard_cmd.opts.verbose_target(),
                    &neard_cmd.opts.o11y,
                )?;
            }
            NeardSubCommand::StatePartsDumpCheck(cmd) => {
                cmd.run()?;
            }
            NeardSubCommand::ReplayArchive(cmd) => {
                cmd.run(&home_dir, genesis_validation)?;
            }
        };
        Ok(())
    }
}

#[derive(clap::Parser)]
pub(super) struct StateViewerCommand {
    /// By default state viewer opens rocks DB in the read only mode, which allows it to run
    /// multiple instances in parallel and be sure that no unintended changes get written to the DB.
    /// In case an operation needs to write to caches, a read-write mode may be needed.
    #[clap(long, short = 'w')]
    readwrite: bool,
    /// What store temperature should the state viewer open. Allowed values are hot and cold but
    /// cold is only available when cold_store is configured.
    /// Cold temperature actually means the split store will be used.
    #[clap(long, short = 't', default_value = "hot")]
    store_temperature: near_store::Temperature,
    #[clap(subcommand)]
    subcmd: StateViewerSubCommand,
}

#[derive(clap::Parser, Debug)]
struct NeardOpts {
    /// Sets verbose logging for the given target, or for all targets if no
    /// target is given.
    #[clap(long, name = "target")]
    verbose: Option<Option<String>>,
    /// Directory for config and data.
    #[clap(long, value_parser, default_value_os = crate::DEFAULT_HOME.as_os_str())]
    home: PathBuf,
    /// Skips consistency checks of genesis.json (and records.json) upon startup.
    /// Let's you start `neard` slightly faster.
    #[clap(long)]
    unsafe_fast_startup: bool,
    /// Enables export of span data using opentelemetry protocol.
    #[clap(flatten)]
    o11y: near_o11y::Options,
}

impl NeardOpts {
    // TODO(nikurt): Delete in 1.38 or later.
    pub fn verbose_target(&self) -> Option<&str> {
        self.verbose.as_ref().map(|inner| {
            tracing::error!(target: "neard", "--verbose flag is deprecated, please use RUST_LOG or log_config.json instead.");
            inner.as_ref().map_or("", String::as_str)
        })
    }
}

#[derive(clap::Parser)]
pub(super) enum NeardSubCommand {
    /// Initializes NEAR configuration
    Init(InitCmd),

    /// Runs NEAR node
    Run(RunCmd),

    /// Sets up local configuration with all necessary files (validator key, node key, genesis and
    /// config)
    Localnet(LocalnetCmd),

    /// View DB state.
    #[clap(name = "view-state", alias = "view_state")]
    StateViewer(StateViewerCommand),

    /// Verify proofs
    #[clap(alias = "verify_proof")]
    VerifyProof(VerifyProofSubCommand),

    /// Connects to a NEAR node and sends ping messages to the accounts it sends
    /// us after the handshake is completed, printing stats to stdout.
    Ping(PingCommand),

    /// Mirror transactions from a source chain to a test chain with state forked
    /// from it, reproducing traffic and state as closely as possible.
    Mirror(MirrorCommand),

    /// Amend a genesis/records file created by `dump-state`.
    AmendGenesis(AmendGenesisCommand),

    /// Testing tool for cold storage
    ColdStore(ColdStoreCommand),

    /// Connects to a NEAR node and sends state parts requests after the handshake is completed.
    StateParts(StatePartsCommand),

    /// Flat storage related tooling.
    FlatStorage(FlatStorageCommand),

    /// validate config files including genesis.json and config.json
    ValidateConfig(ValidateConfigCommand),

    /// reset the head of the chain locally to the prev block of current head
    UndoBlock(UndoBlockCommand),

    /// Set of commands to run on database
    Database(DatabaseCommand),

    /// Resets the network into a forked network at the given block height and state.
    ForkNetwork(ForkNetworkCommand),

    /// Check completeness of dumped state parts of an epoch
    StatePartsDumpCheck(StatePartsDumpCheckCommand),

    /// Replays the blocks in the chain from an archival node.
    ReplayArchive(ReplayArchiveCommand),
}

#[derive(clap::Parser)]
pub(super) struct InitCmd {
    /// Download the verified NEAR genesis file automatically.
    #[clap(long)]
    download_genesis: bool,
    /// Download the verified NEAR config file automatically.
    /// Can be one of "validator", "rpc", and "archival".
    /// If flag is present with no value, defaults to "validator".
    #[clap(long, default_missing_value = "validator", num_args(0..=1))]
    download_config: Option<String>,
    /// Makes block production fast (TESTING ONLY).
    #[clap(long)]
    fast: bool,
    /// Account ID for the validator key.
    #[clap(long)]
    account_id: Option<String>,
    /// Chain ID, by default creates new random.
    #[clap(long, value_parser(clap::builder::NonEmptyStringValueParser::new()))]
    chain_id: Option<String>,
    /// Specify a custom download URL for the genesis file.
    #[clap(long)]
    download_genesis_url: Option<String>,
    /// Specify a custom download URL for the records file.
    #[clap(long)]
    download_records_url: Option<String>,
    /// Specify a custom download URL for the config file.
    #[clap(long)]
    download_config_url: Option<String>,
    /// Genesis file to use when initializing testnet (including downloading).
    #[clap(long)]
    genesis: Option<String>,
    /// Initialize boots nodes in <node_key>@<ip_addr> format separated by commas
    /// to bootstrap the network and store them in config.json
    #[clap(long)]
    boot_nodes: Option<String>,
    /// Number of shards to initialize the chain with.
    #[clap(long, default_value = "1")]
    num_shards: NumShards,
    /// Specify private key generated from seed (TESTING ONLY).
    #[clap(long)]
    test_seed: Option<String>,
    /// Customize max_gas_burnt_view runtime limit.  If not specified, value
    /// from genesis configuration will be taken.
    #[clap(long)]
    max_gas_burnt_view: Option<Gas>,
}

/// Warns if unsupported build of the executable is used on mainnet or testnet.
///
/// Verifies that when running on mainnet or testnet chain a neard binary built
/// with `make release` command is used.  That Makefile targets enable
/// optimisation options which aren’t enabled when building with different
/// methods and is the only officially supported method of building the binary
/// to run in production.
///
/// The detection is done by checking that `NEAR_RELEASE_BUILD` environment
/// variable was set to `release` during compilation (which is what Makefile
/// sets) and that neither `nightly` nor `nightly_protocol` features are
/// enabled.
fn check_release_build(chain: &str) {
    let is_release_build = option_env!("NEAR_RELEASE_BUILD") == Some("release")
        && !cfg!(feature = "nightly")
        && !cfg!(feature = "nightly_protocol");
    if !is_release_build
        && [near_primitives::chains::MAINNET, near_primitives::chains::TESTNET].contains(&chain)
    {
        warn!(
            target: "neard",
            "Running a neard executable which wasn’t built with `make release` \
             command isn’t supported on {}.",
            chain
        );
        warn!(
            target: "neard",
            "Note that `cargo build --release` builds lack optimisations which \
             may be needed to run properly on {}",
            chain
        );
        warn!(
            target: "neard",
            "Consider recompiling the binary using `make release` command.");
    }
}

impl InitCmd {
    pub(super) fn run(self, home_dir: &Path) -> anyhow::Result<()> {
        // TODO: Check if `home` exists. If exists check what networks we already have there.
        if (self.download_genesis || self.download_genesis_url.is_some()) && self.genesis.is_some()
        {
            anyhow::bail!("Please give either --genesis or --download-genesis, not both.");
        }

        if let Some(chain) = self.chain_id.as_ref() {
            check_release_build(chain)
        }

        let download_config_type = if let Some(config_type) = self.download_config.as_deref() {
            Some(DownloadConfigType::from_str(config_type)?)
        } else {
            None
        };

        nearcore::init_configs(
            home_dir,
            self.chain_id,
            self.account_id.and_then(|account_id| account_id.parse().ok()),
            self.test_seed.as_deref(),
            self.num_shards,
            self.fast,
            self.genesis.as_deref(),
            self.download_genesis,
            self.download_genesis_url.as_deref(),
            self.download_records_url.as_deref(),
            download_config_type,
            self.download_config_url.as_deref(),
            self.boot_nodes.as_deref(),
            self.max_gas_burnt_view,
        )
        .context("Failed to initialize configs")
    }
}

#[derive(clap::Parser)]
pub(super) struct RunCmd {
    /// Configure node to run as archival node which prevents deletion of old
    /// blocks.  This is a persistent setting; once client is started as
    /// archival node, it cannot be run in non-archival mode.
    #[clap(long)]
    archive: bool,
    /// Set the boot nodes to bootstrap network from.
    #[clap(long)]
    boot_nodes: Option<String>,
    /// Whether to re-establish connections from the ConnectionStore on startup
    #[clap(long)]
    connect_to_reliable_peers_on_startup: Option<bool>,
    /// Minimum number of peers to start syncing/producing blocks
    #[clap(long)]
    min_peers: Option<usize>,
    /// Customize network listening address (useful for running multiple nodes on the same machine).
    #[clap(long)]
    network_addr: Option<SocketAddr>,
    /// Set this to false to only produce blocks when there are txs or receipts (default true).
    #[clap(long)]
    produce_empty_blocks: Option<bool>,
    /// Customize RPC listening address (useful for running multiple nodes on
    /// the same machine).  Ignored if ‘--disable-rpc’ is given.
    #[cfg(feature = "json_rpc")]
    #[clap(long)]
    rpc_addr: Option<String>,
    /// Export prometheus metrics on an additional listening address, which is useful
    /// for having separate access restrictions for the RPC and prometheus endpoints.
    /// Ignored if RPC http server is disabled, see 'rpc_addr'.
    #[cfg(feature = "json_rpc")]
    #[clap(long)]
    rpc_prometheus_addr: Option<String>,
    /// Disable the RPC endpoint.  This is a no-op on builds which don’t support
    /// RPC endpoint.
    #[clap(long)]
    #[allow(dead_code)]
    disable_rpc: bool,
    /// Customize telemetry url.
    #[clap(long)]
    telemetry_url: Option<String>,
    /// Customize max_gas_burnt_view runtime limit.  If not specified, either
    /// value given at ‘init’ (i.e. present in config.json) or one from genesis
    /// configuration will be taken.
    #[clap(long)]
    max_gas_burnt_view: Option<Gas>,
}

impl RunCmd {
    pub(super) fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
        verbose_target: Option<&str>,
        o11y_opts: &near_o11y::Options,
    ) {
        // Load configs from home.
        let mut near_config = nearcore::config::load_config(home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        check_release_build(&near_config.client_config.chain_id);

        // Set current version in client config.
        near_config.client_config.version = crate::neard_version();
        // Override some parameters from command line.
        if let Some(produce_empty_blocks) = self.produce_empty_blocks {
            near_config.client_config.produce_empty_blocks = produce_empty_blocks;
        }
        if let Some(connect_to_reliable_peers_on_startup) =
            self.connect_to_reliable_peers_on_startup
        {
            near_config.network_config.connect_to_reliable_peers_on_startup =
                connect_to_reliable_peers_on_startup;
        }
        if let Some(boot_nodes) = self.boot_nodes {
            if !boot_nodes.is_empty() {
                near_config.network_config.peer_store.boot_nodes = boot_nodes
                    .split(',')
                    .map(|chunk| chunk.parse().expect("Failed to parse PeerInfo"))
                    .collect();
            }
        }
        if let Some(min_peers) = self.min_peers {
            near_config.client_config.min_num_peers = min_peers;
        }
        if let Some(network_addr) = self.network_addr {
            near_config.network_config.node_addr =
                Some(near_network::tcp::ListenerAddr::new(network_addr));
        }
        #[cfg(feature = "json_rpc")]
        if self.disable_rpc {
            near_config.rpc_config = None;
        } else {
            if let Some(rpc_addr) = self.rpc_addr {
                near_config.rpc_config.get_or_insert(Default::default()).addr =
                    tcp::ListenerAddr::new(rpc_addr.parse().unwrap());
            }
            if let Some(rpc_prometheus_addr) = self.rpc_prometheus_addr {
                near_config.rpc_config.get_or_insert(Default::default()).prometheus_addr =
                    Some(rpc_prometheus_addr);
            }
        }
        if let Some(telemetry_url) = self.telemetry_url {
            if !telemetry_url.is_empty() {
                near_config.telemetry_config.endpoints.push(telemetry_url);
            }
        }
        if self.archive {
            near_config.client_config.archive = true;
        }
        if self.max_gas_burnt_view.is_some() {
            near_config.client_config.max_gas_burnt_view = self.max_gas_burnt_view;
        }

        #[cfg(feature = "sandbox")]
        {
            if near_config.client_config.chain_id == near_primitives::chains::MAINNET
                || near_config.client_config.chain_id == near_primitives::chains::TESTNET
            {
                eprintln!(
                    "Sandbox node can only run dedicate localnet, cannot connect to a network"
                );
                std::process::exit(1);
            }
        }

        let (tx_crash, mut rx_crash) = broadcast::channel::<()>(16);
        let (tx_config_update, rx_config_update) =
            broadcast::channel::<Result<UpdateableConfigs, Arc<UpdateableConfigLoaderError>>>(16);
        let sys = actix::System::new();

        sys.block_on(async move {
            // Initialize the subscriber that takes care of both logging and tracing.
            let _subscriber_guard = default_subscriber_with_opentelemetry(
                make_env_filter(verbose_target).unwrap(),
                o11y_opts,
                near_config.client_config.chain_id.clone(),
                near_config.network_config.node_key.public_key().clone(),
                near_config.network_config.validator.account_id(),
            )
            .await
            .global();

            let updateable_configs = nearcore::dyn_config::read_updateable_configs(home_dir)
                .unwrap_or_else(|e| panic!("Error reading dynamic configs: {:#}", e));
            let mut updateable_config_loader =
                UpdateableConfigLoader::new(updateable_configs.clone(), tx_config_update);
            let config_updater = ConfigUpdater::new(rx_config_update);

            let nearcore::NearNode {
                rpc_servers,
                cold_store_loop_handle,
                mut state_sync_dumper,
                flat_state_migration_handle,
                resharding_handle,
                ..
            } = nearcore::start_with_config_and_synchronization(
                home_dir,
                near_config,
                Some(tx_crash),
                Some(config_updater),
            )
            .expect("start_with_config");

            let sig = loop {
                let sig = wait_for_interrupt_signal(home_dir, &mut rx_crash).await;
                if sig == "SIGHUP" {
                    let maybe_updateable_configs =
                        nearcore::dyn_config::read_updateable_configs(home_dir);
                    updateable_config_loader.reload(maybe_updateable_configs);
                } else {
                    break sig;
                }
            };
            warn!(target: "neard", "{}, stopping... this may take a few minutes.", sig);
            if let Some(handle) = cold_store_loop_handle {
                handle.stop()
            }
            state_sync_dumper.stop();
            resharding_handle.stop();
            flat_state_migration_handle.stop();
            futures::future::join_all(rpc_servers.iter().map(|(name, server)| async move {
                server.stop(true).await;
                debug!(target: "neard", "{} server stopped", name);
            }))
            .await;
            actix::System::current().stop();
            // Disable the subscriber to properly shutdown the tracer.
            near_o11y::reload(Some("error"), None, Some("off")).unwrap();
        });
        sys.run().unwrap();
        info!(target: "neard", "Waiting for RocksDB to gracefully shutdown");
        RocksDB::block_until_all_instances_are_dropped();
    }
}

#[cfg(not(unix))]
async fn wait_for_interrupt_signal(_home_dir: &Path, mut _rx_crash: &Receiver<()>) -> &str {
    // TODO(#6372): Support graceful shutdown on windows.
    tokio::signal::ctrl_c().await.unwrap();
    "Ctrl+C"
}

#[cfg(unix)]
async fn wait_for_interrupt_signal(_home_dir: &Path, rx_crash: &mut Receiver<()>) -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let mut sighup = signal(SignalKind::hangup()).unwrap();

    tokio::select! {
         _ = sigint.recv()  => "SIGINT",
         _ = sigterm.recv() => "SIGTERM",
         _ = sighup.recv() => "SIGHUP",
         _ = rx_crash.recv() => "ClientActor died",
    }
}

#[derive(clap::Parser)]
pub(super) struct LocalnetCmd {
    /// Number of shards to initialize the localnet with.
    #[clap(short = 's', long, default_value = "1")]
    shards: NumShards,
    /// Number of validators to initialize the localnet with.
    #[clap(short = 'v', long, alias = "v", default_value = "4")]
    validators: NumSeats,
    /// Number of non-validator archival nodes to initialize the localnet with.
    /// They are created in addition to the other non-validators.
    /// The archival nodes will have split storage (hot + cold) and they will track all shards.
    #[clap(long, default_value = "0")]
    non_validators_archival: NumSeats,
    /// Number of non-validator RPC nodes to initialize the localnet with.
    /// They are created in addition to the other non-validators.
    /// The RPC nodes will track all shards.
    #[clap(long, default_value = "0")]
    non_validators_rpc: NumSeats,
    /// Number of non-validators to initialize the localnet with.
    /// Prefer `--non_validators_archival` and `--non_validators_rpc`
    /// to create non-validator nodes configured for the archival and RPC roles.
    #[clap(short = 'n', long, alias = "n", default_value = "0")]
    non_validators: NumSeats,
    /// Prefix for the directory name for each node with (e.g. ‘node’ results in
    /// ‘node0’, ‘node1’, ...)
    #[clap(long, default_value = "node")]
    prefix: String,
    /// Comma separated list of shards to track, the word 'all' to track all shards or the word 'none' to track no shards.
    #[clap(long, default_value = "all")]
    tracked_shards: String,
}

impl LocalnetCmd {
    fn parse_tracked_shards(tracked_shards: &str, num_shards: NumShards) -> Vec<u64> {
        if tracked_shards.to_lowercase() == "all" {
            return (0..num_shards).collect();
        }
        if tracked_shards.to_lowercase() == "none" {
            return vec![];
        }
        tracked_shards
            .split(',')
            .map(|shard_id| shard_id.parse::<u64>().expect("Shard id must be an integer"))
            .collect()
    }

    pub(super) fn run(self, home_dir: &Path) {
        let tracked_shards = Self::parse_tracked_shards(&self.tracked_shards, self.shards);
        nearcore::config::init_localnet_configs(
            home_dir,
            self.shards,
            self.validators,
            self.non_validators_archival,
            self.non_validators_rpc,
            self.non_validators,
            &self.prefix,
            tracked_shards,
        );
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum VerifyProofError {
    #[error("invalid outcome root proof")]
    InvalidOutcomeRootProof,
    #[error("invalid block hash proof")]
    InvalidBlockHashProof,
}

#[derive(clap::Parser)]
pub struct VerifyProofSubCommand {
    #[clap(long)]
    json_file_path: String,
}

impl VerifyProofSubCommand {
    /// Verifies light client transaction proof (result of the EXPERIMENTAL_light_client_proof RPC call).
    /// Returns the Hash and height of the block that transaction belongs to, and root of the light block merkle tree.
    pub fn run(self) -> ((CryptoHash, u64), CryptoHash) {
        let file = File::open(Path::new(self.json_file_path.as_str()))
            .with_context(|| "Could not open proof file.")
            .unwrap();
        let reader = BufReader::new(file);
        let light_client_rpc_response: Value =
            serde_json::from_reader(reader).with_context(|| "Failed to deserialize JSON.").unwrap();
        Self::verify_json(light_client_rpc_response).unwrap()
    }

    pub fn verify_json(
        light_client_rpc_response: Value,
    ) -> Result<((CryptoHash, u64), CryptoHash), VerifyProofError> {
        let light_client_proof: RpcLightClientExecutionProofResponse =
            serde_json::from_value(light_client_rpc_response["result"].clone()).unwrap();

        println!(
            "Verifying light client proof for txn id: {:?}",
            light_client_proof.outcome_proof.id
        );
        let outcome_hashes = light_client_proof.outcome_proof.to_hashes();
        println!("Hashes of the outcome are: {:?}", outcome_hashes);

        let outcome_hash = CryptoHash::hash_borsh(&outcome_hashes);
        println!("Hash of the outcome is: {:?}", outcome_hash);

        let outcome_shard_root =
            compute_root_from_path(&light_client_proof.outcome_proof.proof, outcome_hash);
        println!("Shard outcome root is: {:?}", outcome_shard_root);
        let block_outcome_root = compute_root_from_path(
            &light_client_proof.outcome_root_proof,
            CryptoHash::hash_borsh(outcome_shard_root),
        );
        println!("Block outcome root is: {:?}", block_outcome_root);

        if light_client_proof.block_header_lite.inner_lite.outcome_root != block_outcome_root {
            println!(
                "{}",
                yansi::Paint::default(format!(
                    "ERROR: computed outcome root: {:?} doesn't match the block one {:?}.",
                    block_outcome_root,
                    light_client_proof.block_header_lite.inner_lite.outcome_root
                ))
                .fg(yansi::Color::Red)
                .bold()
            );
            return Err(VerifyProofError::InvalidOutcomeRootProof);
        }
        let block_hash = light_client_proof.outcome_proof.block_hash;

        if light_client_proof.block_header_lite.hash()
            != light_client_proof.outcome_proof.block_hash
        {
            println!(
                "{}",
                yansi::Paint::default(format!(
                    "ERROR: block hash from header lite {:?} doesn't match the one from outcome proof {:?}",
                    light_client_proof.block_header_lite.hash(),
                    light_client_proof.outcome_proof.block_hash
                )).fg(yansi::Color::Red).bold()
            );
            return Err(VerifyProofError::InvalidBlockHashProof);
        } else {
            println!(
                "{}",
                yansi::Paint::green(format!("Block hash matches {:?}", block_hash)).bold()
            );
        }

        // And now check that block exists in the light client.

        let light_block_merkle_root =
            compute_root_from_path(&light_client_proof.block_proof, block_hash);

        println!(
            "Please verify that your light block has the following block merkle root: {:?}",
            light_block_merkle_root
        );
        println!(
            "OR verify that block with this hash {:?} is in the chain at this height {:?}",
            block_hash, light_client_proof.block_header_lite.inner_lite.height
        );
        Ok((
            (block_hash, light_client_proof.block_header_lite.inner_lite.height),
            light_block_merkle_root,
        ))
    }
}

fn make_env_filter(verbose: Option<&str>) -> Result<EnvFilter, BuildEnvFilterError> {
    let env_filter = EnvFilterBuilder::from_env().verbose(verbose).finish()?;
    // Sandbox node can log to sandbox logging target via sandbox_debug_log host function.
    // This is hidden by default so we enable it for sandbox node.
    let env_filter = if cfg!(feature = "sandbox") {
        env_filter.add_directive("sandbox=debug".parse().unwrap())
    } else {
        env_filter
    };
    Ok(env_filter)
}

#[derive(clap::Parser)]
pub(super) struct ValidateConfigCommand {}

impl ValidateConfigCommand {
    pub(super) fn run(
        &self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        nearcore::config::load_config(home_dir, genesis_validation)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{CryptoHash, NeardCmd, NeardSubCommand, VerifyProofError, VerifyProofSubCommand};
    use clap::Parser;
    use std::str::FromStr;

    #[test]
    fn optional_values() {
        let cmd = NeardCmd::parse_from(&["test", "init", "--chain-id=testid", "--fast"]);
        if let NeardSubCommand::Init(scmd) = cmd.subcmd {
            assert_eq!(scmd.chain_id, Some("testid".to_string()));
            assert!(scmd.fast);
        } else {
            panic!("incorrect subcommand");
        }
    }

    #[test]
    fn equal_no_value_syntax() {
        assert!(NeardCmd::try_parse_from(&[
            "test",
            "init",
            // * This line currently fails to be parsed (= without a value)
            "--chain-id=",
            "--test-seed=alice.near",
            "--account-id=test.near",
            "--fast"
        ])
        .is_err());
    }

    #[test]
    fn verify_proof_test() {
        assert_eq!(
            VerifyProofSubCommand::verify_json(
                serde_json::from_slice(include_bytes!("../res/proof_example.json")).unwrap()
            )
            .unwrap(),
            (
                (
                    CryptoHash::from_str("HqZHDTHSqH6Az22SZgFUjodGFDtfC2qSt4v9uYFpLuFC").unwrap(),
                    38 as u64
                ),
                CryptoHash::from_str("BWwZdhAhjAgKxZ5ycqn1CvXads5DjPMfj4kRdc1rWit8").unwrap()
            )
        );

        // Proof with a wrong outcome (as user specified wrong shard).
        assert_eq!(
            VerifyProofSubCommand::verify_json(
                serde_json::from_slice(include_bytes!("../res/invalid_proof.json")).unwrap()
            )
            .unwrap_err(),
            VerifyProofError::InvalidOutcomeRootProof
        );
    }
}
