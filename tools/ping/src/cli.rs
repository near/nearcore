use anyhow::Context;
use near_network::types::PeerInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

#[derive(clap::Parser)]
pub struct PingCommand {
    #[clap(long)]
    chain_id: String,
    #[clap(long)]
    /// genesis hash to use in the Handshake we send. This must be provided if --chain-id
    /// is not "mainnet" or "testnet"
    genesis_hash: Option<String>,
    #[clap(long)]
    /// head height to use in the Handshake we send. This must be provided if --chain-id
    /// is not "mainnet" or "testnet"
    head_height: Option<u64>,
    /// Protocol version to advertise in our handshake
    #[clap(long)]
    protocol_version: Option<u32>,
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
    /// filename to append CSV data to
    #[clap(long)]
    latencies_csv_file: Option<PathBuf>,
    /// number of seconds to wait for incoming data before timing out
    #[clap(long)]
    recv_timeout_seconds: Option<u32>,
    /// Listen address for prometheus metrics.
    #[clap(long, default_value = "0.0.0.0:9000")]
    prometheus_addr: String,
    /// Just establish a handshake connection and exit without sending pings.
    #[clap(long)]
    just_handshake: bool,
}

fn display_stats(stats: &mut [(PeerId, crate::PingStats)]) {
    let mut peer_width = "peer".len();
    for (peer, _) in stats.iter() {
        peer_width = std::cmp::max(peer_width, format!("{}", peer).len());
    }
    // the ones that never responded should end up at the top with this sorting, which is a
    // little weird, but we can fix it later.
    stats.sort_by(|(_, left), (_, right)| left.average_latency.cmp(&right.average_latency));
    println!(
        "{:<peer_width$} | {:<10} | {:<10} | {:<17} | {:<17} | {:<17}",
        "peer",
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
        let min_latency: Duration = stats.min_latency.try_into().unwrap();
        let max_latency: Duration = stats.max_latency.try_into().unwrap();
        let average_latency: Duration = stats.average_latency.try_into().unwrap();
        println!(
            "{:<peer_width$} | {:<10} | {:<10} | {:<17?} | {:<17?} | {:<17?}",
            peer, stats.pings_sent, stats.pongs_received, min_latency, max_latency, average_latency
        );
    }
}

// TODO: Refactor this struct into a separate crate.
pub struct ChainInfo {
    pub chain_id: &'static str,
    pub genesis_hash: CryptoHash,
}

pub static CHAIN_INFO: &[ChainInfo] = &[
    ChainInfo {
        chain_id: near_primitives::chains::MAINNET,
        genesis_hash: CryptoHash([
            198, 253, 249, 28, 142, 130, 248, 249, 23, 204, 25, 117, 233, 222, 28, 100, 190, 17,
            137, 158, 50, 29, 253, 245, 254, 188, 251, 183, 49, 63, 20, 134,
        ]),
    },
    ChainInfo {
        chain_id: near_primitives::chains::TESTNET,
        genesis_hash: CryptoHash([
            215, 132, 218, 90, 158, 94, 102, 102, 133, 22, 193, 154, 128, 149, 68, 143, 197, 74,
            34, 162, 137, 113, 220, 51, 15, 0, 153, 223, 148, 55, 148, 16,
        ]),
    },
];

impl PingCommand {
    pub fn run(&self) -> anyhow::Result<()> {
        tracing::warn!(target: "ping", "the ping command is not stable, and may be removed or changed arbitrarily at any time");

        let mut chain_info = None;
        for info in CHAIN_INFO {
            if &info.chain_id == &self.chain_id {
                chain_info = Some(info);
                break;
            }
        }

        let genesis_hash = if let Some(h) = &self.genesis_hash {
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
                    &self.chain_id
                ),
            }
        };

        let peer = match PeerInfo::from_str(&self.peer) {
            Ok(p) => p,
            Err(e) => anyhow::bail!("Could not parse --peer {}: {:?}", &self.peer, e),
        };
        if peer.addr.is_none() {
            anyhow::bail!("--peer should be in the form [public key]@[socket addr]");
        }
        let csv =
            if let Some(filename) = &self.latencies_csv_file {
                Some(crate::csv::LatenciesCsv::open(filename).with_context(|| {
                    format!("Couldn't open latencies CSV file at {:?}", filename)
                })?)
            } else {
                None
            };
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let mut stats = Vec::new();
            crate::ping_via_node(
                &self.chain_id,
                genesis_hash,
                self.head_height.unwrap_or(0),
                self.protocol_version,
                peer.id.clone(),
                peer.addr.unwrap(),
                self.ttl,
                self.ping_frequency_millis,
                self.recv_timeout_seconds.unwrap_or(5),
                csv,
                &mut stats,
                &self.prometheus_addr,
                self.just_handshake,
            )
            .await?;
            if !self.just_handshake {
                display_stats(&mut stats);
            }
            Ok(())
        })
    }
}
