use crate::cli::start_with_config;
use clap::Clap;
use near_chain_configs::GenesisValidationMode;
use nearcore::get_default_home;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

mod cli;

#[derive(Clap)]
struct StressTestCmd {
    #[clap(flatten)]
    opts: NeardOpts,
    #[clap(subcommand)]
    subcmd: NeardSubCommand,
}

#[derive(Clap)]
enum NeardSubCommand {
    #[clap(name = "blocks")]
    Blocks(BlocksCmd),
    #[clap(name = "chunks")]
    Chunks(ChunksCmd),
}

#[derive(Clap)]
struct BlocksCmd {
    requested_blocks_per_sec: usize,
}

#[derive(Clap)]
struct ChunksCmd {
    requested_chunks_per_sec: usize,
}

#[derive(Clap)]
struct NeardOpts {
    #[clap(long)]
    home: Option<PathBuf>,
    #[clap(long)]
    boot_nodes: Option<String>,
    #[clap(long)]
    min_peers: Option<usize>,
    #[clap(long)]
    network_addr: Option<SocketAddr>,
    #[clap(long)]
    telemetry_url: Option<String>,
}

fn main() {
    let cmd = StressTestCmd::parse();

    const DEFAULT_RUST_LOG: &'static str =
        "tokio_reactor=info,near=info,stats=info,telemetry=info,\
         delay_detector=info,near-performance-metrics=info,\
         near-rust-allocator-proxy=info,near-stress-test=debug,debug";
    let mut env_filter = EnvFilter::new(DEFAULT_RUST_LOG);

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
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

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    let home_dir = cmd.opts.home.clone().unwrap_or(get_default_home());
    let genesis_validation = GenesisValidationMode::UnsafeFast;

    let mut near_config = nearcore::config::load_config(&home_dir, genesis_validation);

    if let Some(boot_nodes) = cmd.opts.boot_nodes.clone() {
        if !boot_nodes.is_empty() {
            near_config.network_config.boot_nodes = boot_nodes
                .split(',')
                .map(|chunk| chunk.parse().expect("Failed to parse PeerInfo"))
                .collect();
        }
    }
    if let Some(min_peers) = cmd.opts.min_peers.clone() {
        near_config.client_config.min_num_peers = min_peers;
    }
    if let Some(network_addr) = cmd.opts.network_addr {
        near_config.network_config.addr = Some(network_addr);
    }
    if let Some(telemetry_url) = cmd.opts.telemetry_url.clone() {
        if !telemetry_url.is_empty() {
            near_config.telemetry_config.endpoints.push(telemetry_url);
        }
    }

    let sys = actix::System::new();
    sys.block_on(async move {
        start_with_config(&home_dir, near_config, cmd).expect("start_with_config");
    });
    sys.run().unwrap();
}
