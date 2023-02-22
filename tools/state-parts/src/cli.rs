use near_network::types::PeerInfo;
use near_ping::cli::CHAIN_INFO;
use near_primitives::hash::CryptoHash;
use near_primitives::types::ShardId;
use std::str::FromStr;

#[derive(clap::Parser)]
pub struct StatePartsCommand {
    /// The hash of the first block of an epoch of which we are requesting state.
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
                peer.addr.unwrap(),
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
