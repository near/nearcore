use super::{DEFAULT_HOME, NEARD_VERSION, NEARD_VERSION_STRING, PROTOCOL_VERSION};
use clap::{AppSettings, Clap};
use futures::future::FutureExt;
use near_chain_configs::GenesisValidationMode;
use near_primitives::types::{Gas, NumSeats, NumShards};
use near_state_viewer::StateViewerSubCommand;
use nearcore::get_store_path;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::{env, fs, io};
use tracing::metadata::LevelFilter;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

/// NEAR Protocol Node
#[derive(Clap)]
#[clap(version = NEARD_VERSION_STRING.as_str())]
#[clap(setting = AppSettings::SubcommandRequiredElseHelp)]
pub(super) struct NeardCmd {
    #[clap(flatten)]
    opts: NeardOpts,
    #[clap(subcommand)]
    subcmd: NeardSubCommand,
}

impl NeardCmd {
    pub(super) fn parse_and_run() {
        let neard_cmd = Self::parse();
        neard_cmd.opts.init();
        info!(target: "neard", "Version: {}, Build: {}, Latest Protocol: {}", NEARD_VERSION.version, NEARD_VERSION.build, PROTOCOL_VERSION);

        #[cfg(feature = "test_features")]
        {
            error!("THIS IS A NODE COMPILED WITH ADVERSARIAL BEHAVIORS. DO NOT USE IN PRODUCTION.");

            if env::var("ADVERSARY_CONSENT").unwrap_or_default() != "1" {
                error!("To run a node with adversarial behavior enabled give your consent by setting variable:");
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
                warn!("The 'testnet' command has been renamed to 'localnet' and will be removed in the future");
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
                cmd.run(&home_dir, genesis_validation);
            }
        }
    }
}

fn unsafe_reset(command: &str, path: &std::path::Path, what: &str, default: &str) {
    let dir =
        path.to_str().map(|path| shell_escape::unix::escape(path.into())).unwrap_or(default.into());
    warn!(target: "neard", "The ‘{}’ command is deprecated and will be removed in Q3 2022", command);
    warn!(target: "neard", "Use ‘rm -r -- {}’ instead (which is effectively what this command does)", dir);
    info!(target: "neard", "Removing all {} from {}", what, path.display());
    fs::remove_dir_all(path).expect("Removing data failed");
}

#[derive(Clap, Debug)]
struct NeardOpts {
    /// Sets verbose logging for the given target, or for all targets
    /// if "debug" is given.
    #[clap(long, name = "target")]
    verbose: Option<String>,
    /// Directory for config and data.
    #[clap(long, parse(from_os_str), default_value_os = DEFAULT_HOME.as_os_str())]
    home: PathBuf,
    /// Skips consistency checks of the 'genesis.json' file upon startup.
    /// Let's you start `neard` slightly faster.
    #[clap(long)]
    pub unsafe_fast_startup: bool,
}

impl NeardOpts {
    fn init(&self) {
        init_logging(self.verbose.as_deref());
    }
}

#[derive(Clap)]
pub(super) enum NeardSubCommand {
    /// Initializes NEAR configuration
    #[clap(name = "init")]
    Init(InitCmd),
    /// Runs NEAR node
    #[clap(name = "run")]
    Run(RunCmd),
    /// Sets up local configuration with all necessary files (validator key, node key, genesis and
    /// config)
    #[clap(name = "localnet")]
    Localnet(LocalnetCmd),
    /// DEPRECATED: this command has been renamed to 'localnet' and will be removed in a future
    /// release.
    // TODO(#4372): Deprecated since 1.24.  Delete it in a couple of releases in 2022.
    #[clap(name = "testnet")]
    Testnet(LocalnetCmd),
    /// (unsafe) Remove the entire NEAR home directory (which includes the
    /// configuration, genesis files, private keys and data).  This effectively
    /// removes all information about the network.
    #[clap(name = "unsafe_reset_all")]
    UnsafeResetAll,
    /// (unsafe) Remove all the data, effectively resetting node to the genesis state (keeps genesis and
    /// config).
    #[clap(name = "unsafe_reset_data")]
    UnsafeResetData,
    /// View DB state.
    #[clap(name = "view_state")]
    StateViewer(StateViewerSubCommand),
}

#[derive(Clap)]
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
    #[clap(long)]
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

#[derive(Clap)]
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
        // Load configs from home.
        let mut near_config = nearcore::config::load_config(home_dir, genesis_validation);

        check_release_build(&near_config.client_config.chain_id);

        // Set current version in client config.
        near_config.client_config.version = super::NEARD_VERSION.clone();
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

        let sys = actix::System::new();
        sys.block_on(async move {
            let nearcore::NearNode { rpc_servers, .. } =
                nearcore::start_with_config(home_dir, near_config).expect("start_with_config");

            let sig = if cfg!(unix) {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigint = signal(SignalKind::interrupt()).unwrap();
                let mut sigterm = signal(SignalKind::terminate()).unwrap();
                futures::select! {
                    _ = sigint .recv().fuse() => "SIGINT",
                    _ = sigterm.recv().fuse() => "SIGTERM"
                }
            } else {
                tokio::signal::ctrl_c().await.unwrap();
                "Ctrl+C"
            };
            info!(target: "neard", "Got {}, stopping...", sig);
            futures::future::join_all(rpc_servers.iter().map(|(name, server)| async move {
                server.stop(true).await;
                debug!(target: "neard", "{} server stopped", name);
            }))
            .await;
            actix::System::current().stop();
        });
        sys.run().unwrap();
    }
}

#[derive(Clap)]
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
}

impl LocalnetCmd {
    pub(super) fn run(self, home_dir: &Path) {
        nearcore::config::init_testnet_configs(
            home_dir,
            self.shards,
            self.validators,
            self.non_validators,
            &self.prefix,
            false,
        );
    }
}

fn init_logging(verbose: Option<&str>) {
    const DEFAULT_RUST_LOG: &'static str =
        "tokio_reactor=info,near=info,stats=info,telemetry=info,\
         delay_detector=info,near-performance-metrics=info,\
         near-rust-allocator-proxy=info,warn";

    let rust_log = env::var("RUST_LOG");
    let rust_log = rust_log.as_ref().map(String::as_str).unwrap_or(DEFAULT_RUST_LOG);
    let mut env_filter = EnvFilter::new(rust_log);

    if let Some(module) = verbose {
        env_filter = env_filter
            .add_directive("cranelift_codegen=warn".parse().unwrap())
            .add_directive("cranelift_codegen=warn".parse().unwrap())
            .add_directive("h2=warn".parse().unwrap())
            .add_directive("trust_dns_resolver=warn".parse().unwrap())
            .add_directive("trust_dns_proto=warn".parse().unwrap());

        if module.is_empty() {
            env_filter = env_filter.add_directive(LevelFilter::DEBUG.into());
        } else {
            env_filter = env_filter.add_directive(format!("{}=debug", module).parse().unwrap());
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::ENTER
                | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_env_filter(env_filter)
        .with_writer(io::stderr)
        .init();
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
