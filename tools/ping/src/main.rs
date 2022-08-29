use anyhow::Context;
use clap::Parser;
use near_network_primitives::types::PeerInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use near_primitives::types::BlockHeight;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use near_primitives::version::PROTOCOL_VERSION;

#[derive(Parser)]
struct Cli {
    #[clap(long)]
    chain_id: String,
    #[clap(long)]
    /// genesis hash to use in the Handshake we send. This must be provided if --chain-id
    /// is not one of "mainnet", "testnet" or "shardnet"
    genesis_hash: Option<String>,
    #[clap(long)]
    /// head height to use in the Handshake we send. This must be provided if --chain-id
    /// is not one of "mainnet", "testnet" or "shardnet"
    head_height: Option<u64>,
    /// node public key and socket address in the format {pub key}@{socket addr}. e.g.:
    /// ed25519:7PGseFbWxvYVgZ89K1uTJKYoKetWs7BJtbyXDzfbAcqX@127.0.0.1:24567
    #[clap(long)]
    peer: String,
    /// ttl to set on our Routed messages
    #[clap(long, default_value = "100")]
    ttl: u8,
    /// milliseconds to wait between sending pings
    #[clap(long, default_value = "1000")]
    ping_frequency_millis: u64,
    /// line-separated list of accounts to filter on.
    /// We will only try to send pings to these accounts
    #[clap(long)]
    account_filter_file: Option<PathBuf>,
    /// filename to append CSV data to
    #[clap(long)]
    latencies_csv_file: Option<PathBuf>,
    /// Pushgateway address, e.g. http://127.0.0.1:9091
    #[clap(long)]
    metrics_gateway_address: Option<String>,
}

fn display_stats(stats: &mut [(ping::PeerIdentifier, ping::PingStats)], peer_id: &PeerId) {
    let mut acc_width = "account".len();
    for (peer, _) in stats.iter() {
        acc_width = std::cmp::max(acc_width, format!("{}", peer).len());
    }
    // the ones that never responded should end up at the top with this sorting, which is a
    // little weird, but we can fix it later.
    stats.sort_by(|(_, left), (_, right)| left.average_latency.cmp(&right.average_latency));
    println!(
        "{:<acc_width$} | {:<10} | {:<10} | {:<17} | {:<17} | {:<17}",
        "account",
        "num pings",
        "num pongs",
        "min ping latency",
        "max ping latency",
        "avg ping latency"
    );
    for (peer, stats) in stats.iter() {
        if stats.pings_sent == 0 {
            continue;
        }
        println!(
            "{:<acc_width$} | {:<10} | {:<10} | {:<17?} | {:<17?} | {:<17?}{}",
            peer,
            stats.pings_sent,
            stats.pongs_received,
            stats.min_latency,
            stats.max_latency,
            stats.average_latency,
            if peer_id == &peer.peer_id { " <-------------- direct pings" } else { "" }
        );
    }
}

struct ChainInfo {
    chain_id: &'static str,
    genesis_hash: CryptoHash,
    head_height: BlockHeight,
}

static CHAIN_INFO: &[ChainInfo] = &[
    ChainInfo {
        chain_id: "mainnet",
        genesis_hash: CryptoHash([
            198, 253, 249, 28, 142, 130, 248, 249, 23, 204, 25, 117, 233, 222, 28, 100, 190, 17,
            137, 158, 50, 29, 253, 245, 254, 188, 251, 183, 49, 63, 20, 134,
        ]),
        head_height: 71112469,
    },
    ChainInfo {
        chain_id: "testnet",
        genesis_hash: CryptoHash([
            215, 132, 218, 90, 158, 94, 102, 102, 133, 22, 193, 154, 128, 149, 68, 143, 197, 74,
            34, 162, 137, 113, 220, 51, 15, 0, 153, 223, 148, 55, 148, 16,
        ]),
        head_height: 96446588,
    },
    ChainInfo {
        chain_id: "shardnet",
        genesis_hash: CryptoHash([
            23, 22, 21, 53, 29, 32, 253, 218, 219, 182, 221, 220, 200, 18, 11, 102, 161, 16, 96,
            127, 219, 141, 160, 109, 150, 121, 215, 174, 108, 67, 47, 110,
        ]),
        head_height: 1622527,
    },
];

fn parse_account_filter<P: AsRef<Path>>(filename: P) -> std::io::Result<HashSet<AccountId>> {
    let f = File::open(filename.as_ref())?;
    let mut reader = BufReader::new(f);
    let mut line = String::new();
    let mut line_num = 1;
    let mut filter = HashSet::new();
    loop {
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let acc = line.trim().trim_matches('"');
        if acc.len() == 0 {
            continue;
        }
        match AccountId::from_str(acc) {
            Ok(a) => {
                filter.insert(a);
            }
            Err(e) => {
                tracing::warn!(target: "ping", "Could not parse account {} on line {}: {:?}", &line, line_num, e);
            }
        }
        line.clear();
        line_num += 1;
    }
    if filter.is_empty() {
        tracing::warn!(target: "ping", "No accounts parsed from {:?}. Only sending direct pings.", filename.as_ref());
    }
    Ok(filter)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    println!("Protocol version: {}", PROTOCOL_VERSION);

    let _subscriber = near_o11y::default_subscriber(
        near_o11y::EnvFilterBuilder::new(std::env::var("RUST_LOG").unwrap_or("info".to_string()))
            .finish()
            .unwrap(),
        &Default::default(),
    )
    .await
    .global();

    let mut chain_info = None;
    for info in CHAIN_INFO.iter() {
        if &info.chain_id == &args.chain_id {
            chain_info = Some(info);
            break;
        }
    }

    let genesis_hash = if let Some(h) = &args.genesis_hash {
        match CryptoHash::from_str(&h) {
            Ok(h) => h,
            Err(e) => {
                anyhow::bail!("Could not parse --genesis-hash {}: {:?}", &h, e)
            }
        }
    } else {
        match chain_info {
            Some(chain_info) => chain_info.genesis_hash,
            None => anyhow::bail!(
                "--genesis-hash not given, and genesis hash for --chain-id {} not known",
                &args.chain_id
            ),
        }
    };
    let head_height = if let Some(h) = &args.head_height {
        *h
    } else {
        match chain_info {
            Some(chain_info) => chain_info.head_height,
            None => anyhow::bail!(
                "--head-height not given, and genesis hash for --chain-id {} not known",
                &args.chain_id
            ),
        }
    };

    let peer = match PeerInfo::from_str(&args.peer) {
        Ok(p) => p,
        Err(e) => anyhow::bail!("Could not parse --peer {}: {:?}", &args.peer, e),
    };
    if peer.addr.is_none() {
        anyhow::bail!("--peer should be in the form [public key]@[socket addr]");
    }
    let filter = if let Some(filename) = &args.account_filter_file {
        Some(parse_account_filter(filename)?)
    } else {
        None
    };
    let csv = if let Some(filename) = &args.latencies_csv_file {
        Some(
            ping::csv::LatenciesCsv::open(filename)
                .with_context(|| format!("Couldn't open latencies CSV file at {:?}", filename))?,
        )
    } else {
        None
    };
    let mut stats = ping::ping_via_node(
        &args.chain_id,
        genesis_hash,
        head_height,
        peer.id.clone(),
        peer.addr.clone().unwrap(),
        args.ttl,
        args.ping_frequency_millis,
        filter,
        csv,
        args.metrics_gateway_address,
    )
    .await;
    display_stats(&mut stats, &peer.id);
    Ok(())
}
