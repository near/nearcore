mod init;
mod run;
mod testnet;

use std::env;
use std::fs;
use std::io;
use std::path::Path;

use clap::{crate_version, AppSettings, Clap};
#[cfg(feature = "adversarial")]
use tracing::error;
use tracing::info;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::EnvFilter;

use git_version::git_version;
use near_performance_metrics;
use near_primitives::version::{Version, DB_VERSION, PROTOCOL_VERSION};
#[cfg(feature = "memory_stats")]
use near_rust_allocator_proxy::allocator::MyAllocator;
use nearcore::{get_default_home, get_store_path};

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
    Init(init::InitCmd),
    /// Runs NEAR node
    #[clap(name = "run")]
    Run(run::RunCmd),
    /// Sets up testnet configuration with all necessary files (validator key, node key, genesis
    /// and config)
    #[clap(name = "testnet")]
    Testnet(testnet::TestnetCmd),
    /// (unsafe) Remove all the config, keys, data and effectively removing all information about
    /// the network
    #[clap(name = "unsafe_reset_all")]
    UnsafeResetAll,
    /// (unsafe) Remove all the data, effectively resetting node to the genesis state (keeps genesis and
    /// config)
    #[clap(name = "unsafe_reset_data")]
    UnsafeResetData,
}

fn main() {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();
    near_performance_metrics::process::schedule_printing_performance_stats(60);

    let opts = NeardOpts::parse();

    init_logging(opts.verbose.as_deref());
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
        NeardSubCommand::Init(cmd) => cmd.run(&home_dir),
        NeardSubCommand::Testnet(cmd) => cmd.run(&home_dir),
        NeardSubCommand::Run(cmd) => cmd.run(&home_dir),
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
