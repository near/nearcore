//! Helper functions for obtaining chunks from the chunk_distribution_network.

use borsh::BorshDeserialize;
use near_async::messaging::Sender;
use near_chain::{
    blocks_delay_tracker::BlocksDelayTracker, chain::BlockMissingChunks,
    orphan::OrphanMissingChunks,
};
use near_chain_configs::{ChunkDistributionNetworkConfig, ClientConfig};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_primitives::{
    hash::CryptoHash,
    sharding::{PartialEncodedChunk, ShardChunkHeader},
    static_clock::StaticClock,
    types::{EpochId, ShardId},
};
use std::fmt;
use tracing::error;

/// Basic interface for the chunk distribution network.
pub trait ChunkDistributionClient {
    type Error;
    type Response;

    async fn lookup_chunk(
        &self,
        prev_hash: CryptoHash,
        shard_id: ShardId,
    ) -> Result<PartialEncodedChunk, Self::Error>;

    async fn publish_chunk(
        &self,
        chunk: PartialEncodedChunk,
    ) -> Result<Self::Response, Self::Error>;
}

/// Helper struct for the Chunk Distribution Network Feature.
#[derive(Debug, Clone)]
pub struct ChunkDistributionNetwork {
    pub client: reqwest::Client,
    pub config: ChunkDistributionNetworkConfig,
}

impl ChunkDistributionNetwork {
    pub fn new(config: &ClientConfig) -> Option<Self> {
        config
            .chunk_distribution_network
            .as_ref()
            .map(|c| Self { client: reqwest::Client::new(), config: c.clone() })
    }
}

pub fn request_missing_chunks<C>(
    blocks_missing_chunks: Vec<BlockMissingChunks>,
    orphans_missing_chunks: Vec<OrphanMissingChunks>,
    client: C,
    blocks_delay_tracker: &mut BlocksDelayTracker,
    shards_manager_adapter: &Sender<ShardsManagerRequestFromClient>,
) where
    C: ChunkDistributionClient + Clone + 'static,
    C::Error: fmt::Debug,
{
    let now = StaticClock::utc();
    for BlockMissingChunks { prev_hash, missing_chunks } in blocks_missing_chunks {
        for chunk in missing_chunks {
            blocks_delay_tracker.mark_chunk_requested(&chunk, now);
            request_missing_chunk(client.clone(), chunk, shards_manager_adapter, prev_hash);
        }
    }

    for OrphanMissingChunks { missing_chunks, epoch_id, ancestor_hash } in orphans_missing_chunks {
        for chunk in missing_chunks {
            blocks_delay_tracker.mark_chunk_requested(&chunk, now);
            request_orphan_chunk(
                client.clone(),
                chunk,
                shards_manager_adapter,
                epoch_id.clone(),
                ancestor_hash,
            );
        }
    }
}

fn request_missing_chunk<C>(
    client: C,
    chunk: ShardChunkHeader,
    adapter: &Sender<ShardsManagerRequestFromClient>,
    prev_hash: CryptoHash,
) where
    C: ChunkDistributionClient + 'static,
    C::Error: fmt::Debug,
{
    let shard_id = chunk.shard_id();
    let thread_local_shards_manager_adapter = adapter.clone();
    near_performance_metrics::actix::spawn("ChunkDistributionNetwork", async move {
        match client.lookup_chunk(prev_hash, shard_id).await {
            Ok(chunk) => {
                thread_local_shards_manager_adapter
                    .send(ShardsManagerRequestFromClient::ProcessPartialEncodedChunk(chunk));
            }
            Err(err) => {
                error!(target: "client", ?err, "Failed to find chunk via Chunk Distribution Network");
                thread_local_shards_manager_adapter.send(
                    ShardsManagerRequestFromClient::RequestChunks {
                        chunks_to_request: vec![chunk],
                        prev_hash,
                    },
                );
            }
        }
    });
}

fn request_orphan_chunk<C>(
    client: C,
    chunk: ShardChunkHeader,
    adapter: &Sender<ShardsManagerRequestFromClient>,
    epoch_id: EpochId,
    ancestor_hash: CryptoHash,
) where
    C: ChunkDistributionClient + 'static,
    C::Error: fmt::Debug,
{
    let shard_id = chunk.shard_id();
    let thread_local_shards_manager_adapter = adapter.clone();
    near_performance_metrics::actix::spawn("ChunkDistributionNetwork", async move {
        match client.lookup_chunk(ancestor_hash, shard_id).await {
            Ok(chunk) => {
                thread_local_shards_manager_adapter
                    .send(ShardsManagerRequestFromClient::ProcessPartialEncodedChunk(chunk));
            }
            Err(err) => {
                error!(target: "client", ?err, "Failed to find chunk via Chunk Distribution Network");
                thread_local_shards_manager_adapter.send(
                    ShardsManagerRequestFromClient::RequestChunksForOrphan {
                        chunks_to_request: vec![chunk],
                        epoch_id,
                        ancestor_hash,
                    },
                );
            }
        }
    });
}

impl ChunkDistributionClient for ChunkDistributionNetwork {
    type Error = anyhow::Error;
    type Response = reqwest::Response;

    async fn lookup_chunk(
        &self,
        prev_hash: CryptoHash,
        shard_id: ShardId,
    ) -> Result<PartialEncodedChunk, Self::Error> {
        let url = &self.config.uris.get;
        let request = self
            .client
            .get(url)
            .query(&[("prev_hash", prev_hash.to_string()), ("shard_id", shard_id.to_string())]);
        let response = request.send().await?.error_for_status()?;
        let chunk = PartialEncodedChunk::deserialize(&mut response.bytes().await?.as_ref())?;
        Ok(chunk)
    }

    async fn publish_chunk(
        &self,
        chunk: PartialEncodedChunk,
    ) -> Result<Self::Response, Self::Error> {
        let prev_hash = chunk.prev_block();
        let bytes = borsh::to_vec(&chunk)?;
        let url = &self.config.uris.set;
        let request = self
            .client
            .post(url)
            .query(&[
                ("prev_hash", prev_hash.to_string()),
                ("shard_id", chunk.shard_id().to_string()),
            ])
            .body(bytes);
        let response = request.send().await?.error_for_status()?;
        Ok(response)
    }
}
