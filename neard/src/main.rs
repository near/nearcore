use std::convert::TryInto;
use std::env;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::Path;

use clap::{crate_version, AppSettings, Clap};
use near_primitives::types::NumShards;
#[cfg(feature = "adversarial")]
use tracing::error;
use tracing::info;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::EnvFilter;

use actix;
use git_version::git_version;
use near_performance_metrics;
use near_primitives::version::{Version, DB_VERSION, PROTOCOL_VERSION};
#[cfg(feature = "memory_stats")]
use near_rust_allocator_proxy::allocator::MyAllocator;
use nearcore::config::{init_testnet_configs, load_config_without_genesis_records};
use nearcore::genesis_validate::validate_genesis;
use nearcore::{get_default_home, get_store_path, init_configs, start_with_config};

#[macro_use]
extern crate lazy_static;

lazy_static! {
    static ref NEARD_VERSION: Version = Version {
        version: crate_version!().to_string(),
        build: git_version!(fallback = "unknown").to_string(),
    };
    static ref NEARD_VERSION_STRING: String = {
        format!(
            "{} (build {}) (protocol {}) (db {})",
            NEARD_VERSION.version, NEARD_VERSION.build, PROTOCOL_VERSION, DB_VERSION
        )
    };
}

#[cfg(feature = "memory_stats")]
#[global_allocator]
static ALLOC: MyAllocator = MyAllocator;

#[cfg(all(not(feature = "memory_stats"), jemallocator))]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn init_logging(verbose: Option<&str>) {
    let mut env_filter = EnvFilter::new(
        "tokio_reactor=info,near=info,stats=info,telemetry=info,delay_detector=info,\
         near-performance-metrics=info,near-rust-allocator-proxy=info",
    );

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
    } else {
        env_filter = env_filter.add_directive(LevelFilter::WARN.into());
    }

    if let Ok(rust_log) = env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }
    tracing_subscriber::fmt::Subscriber::builder()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_env_filter(env_filter)
        .with_writer(io::stderr)
        .init();
}

/// NEAR Protocol Node
#[derive(Clap)]
#[clap(version = NEARD_VERSION_STRING.as_str())]
#[clap(setting = AppSettings::SubcommandRequiredElseHelp)]
struct NeardOpts {
    /// Verbose logging.
    #[clap(long)]
    verbose: Option<String>,
    /// Directory for config and data (default "~/.near").
    #[clap(long)]
    home: Option<String>,
    #[clap(subcommand)]
    subcmd: NeardSubCommand,
}

#[derive(Clap)]
enum NeardSubCommand {
    /// Initializes NEAR configuration
    #[clap(name = "init")]
    Init(InitCmd),
    /// Runs NEAR node
    #[clap(name = "run")]
    Run(RunCmd),
    /// Sets up testnet configuration with all necessary files (validator key, node key, genesis
    /// and config)
    #[clap(name = "testnet")]
    Testnet(TestnetCmd),
    /// (unsafe) Remove all the config, keys, data and effectively removing all information about
    /// the network
    #[clap(name = "unsafe_reset_all")]
    UnsafeResetAll,
    /// (unsafe) Remove all the data, effectively resetting node to genesis state (keeps genesis and
    /// config)
    #[clap(name = "unsafe_reset_data")]
    UnsafeResetData,
}

#[derive(Clap)]
struct InitCmd {
    /// Download the verified NEAR genesis file automatically.
    #[clap(long)]
    download_genesis: bool,
    /// Makes block production fast (TESTING ONLY).
    #[clap(long)]
    fast: bool,
    /// Account ID for the validator key.
    #[clap(long)]
    account_id: Option<String>,
    /// Chain ID, by default creates new random.
    #[clap(long)]
    chain_id: Option<String>,
    /// Specify a custom download URL for the genesis-file.
    #[clap(long)]
    download_genesis_url: Option<String>,
    /// Genesis file to use when initializing testnet (including downloading).
    #[clap(long)]
    genesis: Option<String>,
    /// Number of shards to initialize the chain with.
    #[clap(long)]
    num_shards: Option<NumShards>,
    /// Specify private key generated from seed (TESTING ONLY).
    #[clap(long)]
    test_seed: Option<String>,
}

#[derive(Clap)]
struct RunCmd {
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
    /// Customize RPC listening address (useful for running multiple nodes on the same machine).
    #[clap(long)]
    rpc_addr: Option<String>,
    /// Customize telemetry url.
    #[clap(long)]
    telemetry_url: Option<String>,
}

#[derive(Clap)]
struct TestnetCmd {
    /// Number of non-validators to initialize the testnet with.
    #[clap(long = "n", default_value = "0")]
    non_validators: u64,
    /// Prefix the directory name for each node with (node results in node0, node1, ...)
    #[clap(long, default_value = "node")]
    prefix: String,
    /// Number of shards to initialize the testnet with.
    #[clap(long, default_value = "4")]
    shards: u64,
    /// Number of validators to initialize the testnet with.
    #[clap(long = "v", default_value = "4")]
    validators: u64,
}

fn main() {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();
    near_performance_metrics::process::schedule_printing_performance_stats(60);

    let opts = NeardOpts::parse();

    init_logging(opts.verbose.as_ref().map(|s| s.as_str()));
    info!(target: "near", "Version: {}, Build: {}, Latest Protocol: {}", NEARD_VERSION.version, NEARD_VERSION.build, PROTOCOL_VERSION);

    #[cfg(feature = "adversarial")]
    {
        error!("THIS IS A NODE COMPILED WITH ADVERSARIAL BEHAVIORS. DO NOT USE IN PRODUCTION.");

        if env::var("ADVERSARY_CONSENT").unwrap_or_else(|_| "".to_string()) != "1" {
            error!("To run a node with adversarial behavior enabled give your consent by setting variable:");
            error!("ADVERSARY_CONSENT=1");
            std::process::exit(1);
        }
    }

    let home_str = opts.home.unwrap_or_else(|| get_default_home());
    let home_dir = Path::new(&home_str);

    match opts.subcmd {
        NeardSubCommand::Init(init) => {
            // TODO: Check if `home` exists. If exists check what networks we already have there.
            let chain_id = init.chain_id.as_ref().map(|s| s.as_str());
            let account_id = init.account_id.as_ref().map(|s| s.as_str());
            let test_seed = init.test_seed.as_ref().map(|s| s.as_str());
            let genesis = init.genesis.as_ref().map(|s| s.as_str());
            let download_genesis_url = init.download_genesis_url.as_ref().map(|s| s.as_str());
            let num_shards = init.num_shards.unwrap_or(1);

            if (init.download_genesis || init.download_genesis_url.is_some())
                && init.genesis.is_some()
            {
                panic!(
                    "Please specify a local genesis file or download the NEAR genesis or specify your own."
                );
            }

            init_configs(
                home_dir,
                chain_id,
                account_id,
                test_seed,
                num_shards,
                init.fast,
                genesis,
                init.download_genesis,
                download_genesis_url,
            );
        }
        NeardSubCommand::Testnet(tnet) => {
            init_testnet_configs(
                home_dir,
                tnet.shards,
                tnet.validators,
                tnet.non_validators,
                &tnet.prefix,
                false,
            );
        }
        NeardSubCommand::Run(run) => {
            // Load configs from home.
            let mut near_config = load_config_without_genesis_records(home_dir);
            validate_genesis(&near_config.genesis);
            // Set current version in client config.
            near_config.client_config.version = NEARD_VERSION.clone();
            // Override some parameters from command line.
            if let Some(produce_empty_blocks) = run.produce_empty_blocks {
                near_config.client_config.produce_empty_blocks = produce_empty_blocks;
            }
            if let Some(boot_nodes) = run.boot_nodes {
                if !boot_nodes.is_empty() {
                    near_config.network_config.boot_nodes = boot_nodes
                        .to_string()
                        .split(',')
                        .map(|chunk| chunk.try_into().expect("Failed to parse PeerInfo"))
                        .collect();
                }
            }
            if let Some(min_peers) = run.min_peers {
                near_config.client_config.min_num_peers = min_peers;
            }
            if let Some(network_addr) = run.network_addr {
                near_config.network_config.addr = Some(network_addr);
            }
            if let Some(rpc_addr) = run.rpc_addr {
                near_config.rpc_config.addr = rpc_addr;
            }
            if let Some(telemetry_url) = run.telemetry_url {
                if !telemetry_url.is_empty() {
                    near_config.telemetry_config.endpoints.push(telemetry_url);
                }
            }
            if run.archive {
                near_config.client_config.archive = true;
            }

            let sys = actix::System::new();
            sys.block_on(async move {
                start_with_config(home_dir, near_config);
            });
            sys.run().unwrap();
        }
        NeardSubCommand::UnsafeResetData => {
            let store_path = get_store_path(home_dir);
            info!(target: "near", "Removing all data from {}", store_path);
            fs::remove_dir_all(store_path).expect("Removing data failed");
        }
        NeardSubCommand::UnsafeResetAll => {
            info!(target: "near", "Removing all data and config from {}", home_dir.to_str().unwrap());
            fs::remove_dir_all(home_dir).expect("Removing data and config failed.");
        }
    }
}
