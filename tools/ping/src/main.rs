use clap::Parser;
use near_network_primitives::types::PeerInfo;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Parser)]
struct Cli {
    /// Path to near home directory with the same chain as the peers we're trying to connect to
    #[clap(long)]
    home: PathBuf,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    let _subscriber = near_o11y::default_subscriber(
        near_o11y::EnvFilterBuilder::from_env().finish().unwrap(),
        &Default::default(),
    )
    .await
    .global();

    let peer = match PeerInfo::from_str(&args.peer) {
        Ok(p) => p,
        Err(e) => anyhow::bail!("Could not parse --peer {}: {:?}", &args.peer, e),
    };
    if peer.addr.is_none() {
        anyhow::bail!("--peer should be in the form [public key]@[socket addr]");
    }
    let stats = ping::ping_nodes(&args.home, peer.id, peer.addr.unwrap(), args.num_pings).await?;
    display_stats(&stats);
    Ok(())
}
