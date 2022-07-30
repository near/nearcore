use anyhow::Context;
use clap::Parser;
use near_crypto::PublicKey;
use std::io::BufRead;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Parser)]
struct Cli {
    /// Path to near home directory with the same chain as the peers we're trying to connect to
    #[clap(long)]
    home: PathBuf,
    /// Path to a  list of public key@socket addr pairs to connect to
    /// e.g.:
    /// $ cat peers.txt
    /// ed25519:6DSjZ8mvsRZDvFqFxo8tCKePG96omXW7eVYVSySmDk8e,127.0.0.1:24568
    /// ed25519:7PGseFbWxvYVgZ89K1uTJKYoKetWs7BJtbyXDzfbAcqX,127.0.0.1:24567
    #[clap(long)]
    peers_file: PathBuf,
    /// Number of ping messages to try to send
    #[clap(long, default_value = "20")]
    num_pings: usize,
}

fn stats_error(stats: &ping::PingStats) -> String {
    match &stats.error {
        Some(e) => format!("{:?}", e),
        None => String::from("operation not completed"),
    }
}

fn display_stats(stats: &[(SocketAddr, ping::PingStats)]) {
    let mut num_conns = 0;
    let mut num_conns_attempted = 0;
    let mut num_handshakes = 0;
    let mut num_handshakes_attempted = 0;

    for (_addr, stats) in stats.iter() {
        num_conns_attempted += 1;
        if stats.connect_latency.is_some() {
            num_conns += 1;
            num_handshakes_attempted += 1;
        }
        if stats.handshake_latency.is_some() {
            num_handshakes += 1;
        }
    }

    if num_conns == num_handshakes && num_conns_attempted == num_handshakes_attempted {
        println!("{} / {} successful connections", num_conns, num_conns_attempted);
    } else {
        println!(
            "{} / {} successful connections, {} / {} successful handshakes",
            num_conns, num_conns_attempted, num_handshakes, num_handshakes_attempted
        );
    }

    println!(
        "{:<18} | {:<16} | {:<17} | {:<10} | {:<10} | {:<17} | {:<17} | {:<17}",
        "addr",
        "connect latency",
        "handshake latency",
        "num pings",
        "num pongs",
        "min ping latency",
        "max ping latency",
        "avg ping latency"
    );
    for (addr, stats) in stats.iter() {
        if stats.connect_latency.is_none() {
            println!("{:<18} | Failed to connect: {:?}", addr, stats_error(stats));
        } else if stats.handshake_latency.is_none() {
            println!(
                "{:<18} | {:<10?} | handshake failed: {:?}",
                addr,
                stats.connect_latency.unwrap(),
                stats_error(stats)
            );
        } else {
            println!(
                "{:<18} | {:<16?} | {:<17?} | {:<10} | {:<10} | {:<17?} | {:<17?} | {:<17?}",
                addr,
                stats.connect_latency.unwrap(),
                stats.handshake_latency.unwrap(),
                stats.pings_sent,
                stats.pongs_received,
                stats.min_latency,
                stats.max_latency,
                stats.average_latency
            );
        }
    }
}

// tries to parse a list of public key@socket addr pairs
// TODO: add an option to read the peers from the DB and just use those?
fn parse_peers<P: AsRef<std::path::Path>>(
    peers_file: P,
) -> anyhow::Result<Vec<(PublicKey, SocketAddr)>> {
    let f = std::fs::File::open(peers_file)?;
    let mut reader = std::io::BufReader::new(f);
    let mut line = String::new();
    let mut peers = Vec::new();

    let mut line_num = 1;
    loop {
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        if line.chars().all(|c| c.is_whitespace()) {
            continue;
        }
        let toks = line.split("@").collect::<Vec<_>>();
        if toks.len() != 2 {
            anyhow::bail!("Invalid line {}:\n{}", line_num, &line);
        }
        let public_key = PublicKey::from_str(toks[0].trim())
            .with_context(|| format!("can't parse public key from {}", toks[0]))?;
        let addr = SocketAddr::from_str(toks[1].trim())
            .with_context(|| format!("can't parse socket addr from {}", toks[1]))?;
        peers.push((public_key, addr));

        line.clear();
        line_num += 1;
    }
    Ok(peers)
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

    let peers = parse_peers(&args.peers_file)?;
    let stats = ping::ping_nodes(&args.home, peers, args.num_pings).await?;
    display_stats(&stats);
    Ok(())
}
