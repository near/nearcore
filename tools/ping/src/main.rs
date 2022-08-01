use clap::Parser;
use near_network_primitives::types::PeerInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use std::str::FromStr;

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
    /// Number of ping messages to try to send
    #[clap(long, default_value = "20")]
    num_pings: usize,
}

fn display_stats(stats: &ping::PingStats) {
    println!(
        "{:<20} | {:<10} | {:<10} | {:<17} | {:<17} | {:<17}",
        "addr",
        "num pings",
        "num pongs",
        "min ping latency",
        "max ping latency",
        "avg ping latency"
    );
    println!(
        "{:<20} | {:<10} | {:<10} | {:<17?} | {:<17?} | {:<17?}{}",
        "hello world",
        stats.pings_sent,
        stats.pongs_received,
        stats.min_latency,
        stats.max_latency,
        stats.average_latency,
        match &stats.error {
            Some(e) => format!(" Error encountered: {:?}", e),
            None => format!(""),
        }
    );
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    let _subscriber = near_o11y::default_subscriber(
        near_o11y::EnvFilterBuilder::from_env().finish().unwrap(),
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
    let stats = ping::ping_nodes(
        &args.chain_id,
        genesis_hash,
        head_height,
        peer.id,
        peer.addr.unwrap(),
        args.num_pings,
    )
    .await?;
    display_stats(&stats);
    Ok(())
}
