use clap::Parser;
use near_network::types::PeerInfo;
use near_primitives::hash::CryptoHash;
use near_primitives::types::ShardId;
use std::str::FromStr;

#[derive(Parser)]
pub struct StatePartsCommand {
    /// block_hash defining the epoch, state of which we are requesting.
    #[clap(long)]
    block_hash: CryptoHash,

    /// shard_id selecting the shard, state of which we are requesting.
    #[clap(long)]
    shard_id: ShardId,

    /// Chain id of the peer.
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

    /// milliseconds to wait between sending state part requests
    #[clap(long, default_value = "1000")]
    request_frequency_millis: u64,

    /// number of seconds to wait for incoming data before timing out
    #[clap(long)]
    recv_timeout_seconds: Option<u32>,

    /// Starting part id for the state requests.
    #[clap(long, default_value = "0")]
    start_part_id: u64,

    /// Number of parts in the state of the shard.
    /// Assuming the tool doesn't have a valid DB and can't determine the number automatically.
    #[clap(long)]
    num_parts: u64,
}

struct ChainInfo {
    chain_id: &'static str,
    genesis_hash: CryptoHash,
}

static CHAIN_INFO: &[ChainInfo] = &[
    ChainInfo {
        chain_id: "mainnet",
        genesis_hash: CryptoHash([
            198, 253, 249, 28, 142, 130, 248, 249, 23, 204, 25, 117, 233, 222, 28, 100, 190, 17,
            137, 158, 50, 29, 253, 245, 254, 188, 251, 183, 49, 63, 20, 134,
        ]),
    },
    ChainInfo {
        chain_id: "testnet",
        genesis_hash: CryptoHash([
            215, 132, 218, 90, 158, 94, 102, 102, 133, 22, 193, 154, 128, 149, 68, 143, 197, 74,
            34, 162, 137, 113, 220, 51, 15, 0, 153, 223, 148, 55, 148, 16,
        ]),
    },
    ChainInfo {
        chain_id: "shardnet",
        genesis_hash: CryptoHash([
            23, 22, 21, 53, 29, 32, 253, 218, 219, 182, 221, 220, 200, 18, 11, 102, 161, 16, 96,
            127, 219, 141, 160, 109, 150, 121, 215, 174, 108, 67, 47, 110,
        ]),
    },
];

impl StatePartsCommand {
    pub fn run(&self) -> anyhow::Result<()> {
        tracing::warn!(target: "state-parts", "the state-parts command is not stable, and may be removed or changed arbitrarily at any time");

        let mut chain_info = None;
        for info in CHAIN_INFO.iter() {
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
        tracing::info!(target: "state-parts", ?genesis_hash, ?peer);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            crate::state_parts_from_node(
                self.block_hash,
                self.shard_id,
                &self.chain_id,
                genesis_hash,
                self.head_height.unwrap_or(0),
                self.protocol_version,
                peer.id.clone(),
                peer.addr.clone().unwrap(),
                self.ttl,
                self.request_frequency_millis,
                self.recv_timeout_seconds.unwrap_or(5),
                self.start_part_id,
                self.num_parts,
            )
            .await?;
            Ok(())
        })
    }
}
