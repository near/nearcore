use crate::log_config_watcher::{LogConfigWatcher, UpdateBehavior};
use clap::{Args, Parser};
use near_chain_configs::GenesisValidationMode;
use near_o11y::{default_subscriber, BuildEnvFilterError, EnvFilterBuilder};
use near_primitives::types::{Gas, NumSeats, NumShards};
use near_state_viewer::StateViewerSubCommand;
use near_store::db::RocksDB;
use nearcore::get_store_path;
use opentelemetry::trace::{Span, Tracer};
use opentelemetry::{global, KeyValue};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{fs, thread};
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;
use tracing::{debug, debug_span, error, info, info_span, span, trace, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// NEAR Protocol Node
#[derive(Parser, Debug)]
#[clap(version = crate::NEARD_VERSION_STRING.as_str())]
#[clap(subcommand_required = true, arg_required_else_help = true)]
pub(super) struct NeardCmd {
    #[clap(flatten)]
    opts: NeardOpts,
    #[clap(subcommand)]
    subcmd: NeardSubCommand,
}

impl NeardCmd {
    pub(super) fn parse_and_run() -> Result<(), RunError> {
        let neard_cmd = Self::parse();
        let verbose = neard_cmd.opts.verbose.as_deref();
        let env_filter =
            EnvFilterBuilder::from_env().verbose(verbose).finish().map_err(RunError::EnvFilter)?;
        // Sandbox node can log to sandbox logging target via sandbox_debug_log host function.
        // This is hidden by default so we enable it for sandbox node.
        let env_filter = if cfg!(feature = "sandbox") {
            env_filter.add_directive("sandbox=debug".parse().unwrap())
        } else {
            env_filter
        };
        let _subscriber = default_subscriber(env_filter).global();

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

        let home_dir = neard_cmd.opts.home;
        let genesis_validation = if neard_cmd.opts.unsafe_fast_startup {
            GenesisValidationMode::UnsafeFast
        } else {
            GenesisValidationMode::Full
        };

        match neard_cmd.subcmd {
            NeardSubCommand::Init(cmd) => cmd.run(&home_dir),
            NeardSubCommand::Localnet(cmd) => cmd.run(&home_dir),
            NeardSubCommand::Testnet(cmd) => {
                warn!(
                    "The 'testnet' command has been renamed to 'localnet' \
                           and will be removed in the future"
                );
                cmd.run(&home_dir);
            }
            NeardSubCommand::Run(cmd) => cmd.run(&home_dir, genesis_validation),

            // TODO(mina86): Remove the command in Q3 2022.
            NeardSubCommand::UnsafeResetData => {
                let store_path = get_store_path(&home_dir);
                unsafe_reset("unsafe_reset_data", &store_path, "data", "<near-home-dir>/data");
            }
            // TODO(mina86): Remove the command in Q3 2022.
            NeardSubCommand::UnsafeResetAll => {
                unsafe_reset("unsafe_reset_all", &home_dir, "data and config", "<near-home-dir>");
            }

            NeardSubCommand::StateViewer(cmd) => {
                cmd.subcmd.run(&home_dir, genesis_validation, cmd.readwrite);
            }

            NeardSubCommand::RecompressStorage(cmd) => {
                cmd.run(&home_dir);
            }
        };
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum RunError {
    #[error("invalid logging directives provided")]
    EnvFilter(#[source] BuildEnvFilterError),
    #[error("could not install a rayon thread pool")]
    RayonInstall(#[source] rayon::ThreadPoolBuildError),
}

#[derive(Parser, Debug)]
pub(super) struct StateViewerCommand {
    /// By default state viewer opens rocks DB in the read only mode, which allows it to run
    /// multiple instances in parallel and be sure that no unintended changes get written to the DB.
    /// In case an operation needs to write to caches, a read-write mode may be needed.
    #[clap(long, short = 'w')]
    readwrite: bool,
    #[clap(subcommand)]
    subcmd: StateViewerSubCommand,
}

fn unsafe_reset(command: &str, path: &std::path::Path, what: &str, default: &str) {
    let dir =
        path.to_str().map(|path| shell_escape::unix::escape(path.into())).unwrap_or(default.into());
    warn!(target: "neard", "The ‘{}’ command is deprecated and will be removed in Q3 2022", command);
    warn!(target: "neard", "Use ‘rm -r -- {}’ instead (which is effectively what this command does)", dir);
    info!(target: "neard", "Removing all {} from {}", what, path.display());
    fs::remove_dir_all(path).expect("Removing data failed");
}

#[derive(Parser, Debug)]
struct NeardOpts {
    /// Sets verbose logging for the given target, or for all targets
    /// if "debug" is given.
    #[clap(long, name = "target")]
    verbose: Option<String>,
    /// Directory for config and data.
    #[clap(long, parse(from_os_str), default_value_os = crate::DEFAULT_HOME.as_os_str())]
    home: PathBuf,
    /// Skips consistency checks of the 'genesis.json' file upon startup.
    /// Let's you start `neard` slightly faster.
    #[clap(long)]
    pub unsafe_fast_startup: bool,
}

#[derive(Parser, Debug)]
pub(super) enum NeardSubCommand {
    /// Initializes NEAR configuration
    Init(InitCmd),
    /// Runs NEAR node
    Run(RunCmd),
    /// Sets up local configuration with all necessary files (validator key, node key, genesis and
    /// config)
    Localnet(LocalnetCmd),
    /// DEPRECATED: this command has been renamed to 'localnet' and will be removed in a future
    /// release.
    // We’re not using clap(alias = "testnet") on Localnet because we want this
    // to be a separate subcommand with a deprecation warning.  TODO(#4372):
    // Deprecated since 1.24.  Delete it in a couple of releases in 2022.
    #[clap(hide = true)]
    Testnet(LocalnetCmd),
    /// (unsafe) Remove the entire NEAR home directory (which includes the
    /// configuration, genesis files, private keys and data).  This effectively
    /// removes all information about the network.
    #[clap(alias = "unsafe_reset_all", hide = true)]
    UnsafeResetAll,
    /// (unsafe) Remove all the data, effectively resetting node to the genesis state (keeps genesis and
    /// config).
    #[clap(alias = "unsafe_reset_data", hide = true)]
    UnsafeResetData,
    /// View DB state.
    #[clap(name = "view-state", alias = "view_state")]
    StateViewer(StateViewerCommand),
    /// Recompresses the entire storage.  This is a slow operation which reads
    /// all the data from the database and writes them down to a new copy of the
    /// database.
    ///
    /// In 1.26 release the compression algorithm for the database has changed
    /// to reduce storage size.  Nodes don’t need to do anything for new data to
    /// take advantage of better compression but existing data may take months
    /// to be recompressed.  This may be an issue for archival nodes which keep
    /// hold of all the old data.
    ///
    /// This command makes it possible to force the recompression as a one-time
    /// operation.  Using it reduces the database even by up to 40% though that
    /// is partially due to database ‘defragmentation’ (whose effects will wear
    /// off in time).  Still, reduction by about 20% even if that’s taken into
    /// account can be expected.
    ///
    /// It’s important to remember however, that this command may take up to
    /// a day to finish in which time the database cannot be used by the node.
    ///
    /// Furthermore, file system where output directory is located needs enough
    /// free space to store the new copy of the database.  It will be smaller
    /// than the original but to be safe one should provision around the same
    /// space as the size of the current `data` directory.
    ///
    /// Finally, because this command is meant only as a temporary migration
    /// tool, it is planned to be removed by the end of 2022.
    #[clap(alias = "recompress_storage")]
    RecompressStorage(RecompressStorageSubCommand),
}

#[derive(Parser, Debug)]
pub(super) struct InitCmd {
    /// Download the verified NEAR genesis file automatically.
    #[clap(long)]
    download_genesis: bool,
    /// Download the verified NEAR config file automatically.
    #[clap(long)]
    download_config: bool,
    /// Makes block production fast (TESTING ONLY).
    #[clap(long)]
    fast: bool,
    /// Account ID for the validator key.
    #[clap(long)]
    account_id: Option<String>,
    /// Chain ID, by default creates new random.
    #[clap(long, forbid_empty_values = true)]
    chain_id: Option<String>,
    /// Specify a custom download URL for the genesis file.
    #[clap(long)]
    download_genesis_url: Option<String>,
    /// Specify a custom download URL for the config file.
    #[clap(long)]
    download_config_url: Option<String>,
    /// Genesis file to use when initializing testnet (including downloading).
    #[clap(long)]
    genesis: Option<String>,
    /// Initialize boots nodes in <node_key>@<ip_addr> format seperated by commas
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
/// sets) and that neither `nightly_protocol` nor `nightly_protocol_features`
/// features are enabled.
fn check_release_build(chain: &str) {
    let is_release_build = option_env!("NEAR_RELEASE_BUILD") == Some("release")
        && !cfg!(feature = "nightly_protocol")
        && !cfg!(feature = "nightly_protocol_features");
    if !is_release_build && ["mainnet", "testnet"].contains(&chain) {
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
    pub(super) fn run(self, home_dir: &Path) {
        // TODO: Check if `home` exists. If exists check what networks we already have there.
        if (self.download_genesis || self.download_genesis_url.is_some()) && self.genesis.is_some()
        {
            error!("Please give either --genesis or --download-genesis, not both.");
            return;
        }

        self.chain_id.as_ref().map(|chain| check_release_build(chain));

        if let Err(e) = nearcore::init_configs(
            home_dir,
            self.chain_id.as_deref(),
            self.account_id.and_then(|account_id| account_id.parse().ok()),
            self.test_seed.as_deref(),
            self.num_shards,
            self.fast,
            self.genesis.as_deref(),
            self.download_genesis,
            self.download_genesis_url.as_deref(),
            self.download_config,
            self.download_config_url.as_deref(),
            self.boot_nodes.as_deref(),
            self.max_gas_burnt_view,
        ) {
            error!("Failed to initialize configs: {:#}", e);
        }
    }
}

#[derive(Parser, Debug)]
pub(super) struct RunCmd {
    /// Keep old blocks in the storage (default false).
    #[clap(long)]
    archive: bool,
    /// Set the boot nodes to bootstrap network from.
    #[clap(long)]
    boot_nodes: Option<String>,
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
    pub(super) fn run(self, home_dir: &Path, genesis_validation: GenesisValidationMode) {
        let root = info_span!("I am groot", i_am_key = "I-am-value");
        let _enter = root.enter();

        // Load configs from home.
        let mut near_config = nearcore::config::load_config(&home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        check_release_build(&near_config.client_config.chain_id);

        // Set current version in client config.
        near_config.client_config.version = crate::neard_version();
        // Override some parameters from command line.
        if let Some(produce_empty_blocks) = self.produce_empty_blocks {
            near_config.client_config.produce_empty_blocks = produce_empty_blocks;
        }
        if let Some(boot_nodes) = self.boot_nodes {
            if !boot_nodes.is_empty() {
                near_config.network_config.boot_nodes = boot_nodes
                    .split(',')
                    .map(|chunk| chunk.parse().expect("Failed to parse PeerInfo"))
                    .collect();
            }
        }
        if let Some(min_peers) = self.min_peers {
            near_config.client_config.min_num_peers = min_peers;
        }
        if let Some(network_addr) = self.network_addr {
            near_config.network_config.addr = Some(network_addr);
        }
        #[cfg(feature = "json_rpc")]
        if self.disable_rpc {
            near_config.rpc_config = None;
        } else {
            if let Some(rpc_addr) = self.rpc_addr {
                near_config.rpc_config.get_or_insert(Default::default()).addr = rpc_addr;
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
            if near_config.client_config.chain_id == "mainnet"
                || near_config.client_config.chain_id == "testnet"
                || near_config.client_config.chain_id == "betanet"
            {
                eprintln!(
                    "Sandbox node can only run dedicate localnet, cannot connect to a network"
                );
                std::process::exit(1);
            }
        }

        let (tx, rx) = oneshot::channel::<()>();
        let sys = actix::System::new();
        let z = root.context();
        sys.block_on(async move {
            let span = info_span!("block_on");
            //            span.set_parent(z);
            let _entered = span.enter();

            let nearcore::NearNode { rpc_servers, .. } =
                nearcore::start_with_config_and_synchronization(home_dir, near_config, Some(tx))
                    .expect("start_with_config");

            let sig = wait_for_interrupt_signal(home_dir, rx).await;
            warn!(target: "neard", "{}, stopping... this may take a few minutes.", sig);
            futures::future::join_all(rpc_servers.iter().map(|(name, server)| async move {
                server.stop(true).await;
                debug!(target: "neard", "{} server stopped", name);
            }))
            .await;
            actix::System::current().stop();
        });
        sys.run().unwrap();
        info!(target: "neard", "Waiting for RocksDB to gracefully shutdown");
        RocksDB::block_until_all_instances_are_dropped();
    }
}

#[cfg(not(unix))]
async fn wait_for_interrupt_signal(_home_dir: &Path, mut _rx_crash: Receiver<()>) -> &str {
    // TODO(#6372): Support graceful shutdown on windows.
    tokio::signal::ctrl_c().await.unwrap();
    "Ctrl+C"
}

#[cfg(unix)]
async fn wait_for_interrupt_signal(home_dir: &Path, mut rx_crash: Receiver<()>) -> &str {
    let watched_path = home_dir.join("log_config.json");
    let log_config_watcher = LogConfigWatcher { watched_path };
    // Apply the logging config file if it exists.
    log_config_watcher.update(UpdateBehavior::UpdateOnlyIfExists);

    use tokio::signal::unix::{signal, SignalKind};
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let mut sighup = signal(SignalKind::hangup()).unwrap();

    loop {
        break tokio::select! {
             _ = sigint.recv()  => "SIGINT",
             _ = sigterm.recv() => "SIGTERM",
             _ = sighup.recv() => {
                log_config_watcher.update(UpdateBehavior::UpdateOrReset);
                continue;
             },
             _ = &mut rx_crash => "ClientActor died",
        };
    }
}

#[derive(Parser, Debug)]
pub(super) struct LocalnetCmd {
    /// Number of non-validators to initialize the localnet with.
    #[clap(long = "n", default_value = "0")]
    non_validators: NumSeats,
    /// Prefix the directory name for each node with (node results in node0, node1, ...)
    #[clap(long, default_value = "node")]
    prefix: String,
    /// Number of shards to initialize the localnet with.
    #[clap(long, default_value = "1")]
    shards: NumShards,
    /// Number of validators to initialize the localnet with.
    #[clap(long = "v", default_value = "4")]
    validators: NumSeats,
    // Whether to create fixed shards accounts (that are tied to a given shard).
    #[clap(long)]
    fixed_shards: bool,
    // Archival nodes
    #[clap(long)]
    archival_nodes: bool,
}

impl LocalnetCmd {
    pub(super) fn run(self, home_dir: &Path) {
        nearcore::config::init_testnet_configs(
            home_dir,
            self.shards,
            self.validators,
            self.non_validators,
            &self.prefix,
            self.archival_nodes,
            self.fixed_shards,
        );
    }
}

#[derive(Args, Debug)]
#[clap(arg_required_else_help = true)]
pub(super) struct RecompressStorageSubCommand {
    /// Directory where to save new storage.
    #[clap(long)]
    output_dir: PathBuf,

    /// Keep data in DBCol::PartialChunks column.  Data in that column can be
    /// reconstructed from DBCol::Chunks is not needed by archival nodes.  This is
    /// always true if node is not an archival node.
    #[clap(long)]
    keep_partial_chunks: bool,

    /// Keep data in DBCol::InvalidChunks column.  Data in that column is only used
    /// when receiving chunks and is not needed to serve archival requests.
    /// This is always true if node is not an archival node.
    #[clap(long)]
    keep_invalid_chunks: bool,

    /// Keep data in DBCol::TrieChanges column.  Data in that column is never used
    /// by archival nodes.  This is always true if node is not an archival node.
    #[clap(long)]
    keep_trie_changes: bool,
}

impl RecompressStorageSubCommand {
    pub(super) fn run(self, home_dir: &Path) {
        warn!(target: "neard", "Recompressing storage; note that this operation may take up to a day to finish.");
        let opts = nearcore::RecompressOpts {
            dest_dir: self.output_dir,
            keep_partial_chunks: self.keep_partial_chunks,
            keep_invalid_chunks: self.keep_invalid_chunks,
            keep_trie_changes: self.keep_trie_changes,
        };
        if let Err(err) = nearcore::recompress_storage(&home_dir, opts) {
            error!("{}", err);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
