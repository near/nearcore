//! Helper functions for obtaining chunks from the chunk_distribution_network.

use crate::client::ChunkDistributionNetwork;
use borsh::BorshDeserialize;
use near_async::messaging::Sender;
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{PartialEncodedChunk, ShardChunkHeader};
use near_primitives::types::EpochId;
use tracing::error;

pub fn request_missing_chunk(
    chunk_distribution: &ChunkDistributionNetwork,
    chunk: ShardChunkHeader,
    adapter: &Sender<ShardsManagerRequestFromClient>,
    prev_hash: CryptoHash,
) {
    let thread_local_client = chunk_distribution.client.clone();
    let url = chunk_distribution.config.uris.get.clone();
    let shard_id = chunk.shard_id().to_string();
    let thread_local_shards_manager_adapter = adapter.clone();
    near_performance_metrics::actix::spawn("ChunkDistributionNetwork", async move {
        match inner_request_chunk(thread_local_client, url, prev_hash, shard_id).await {
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

pub fn request_orphan_chunk(
    chunk_distribution: &ChunkDistributionNetwork,
    chunk: ShardChunkHeader,
    adapter: &Sender<ShardsManagerRequestFromClient>,
    epoch_id: EpochId,
    ancestor_hash: CryptoHash,
) {
    let thread_local_client = chunk_distribution.client.clone();
    let url = chunk_distribution.config.uris.get.clone();
    let shard_id = chunk.shard_id().to_string();
    let thread_local_shards_manager_adapter = adapter.clone();
    near_performance_metrics::actix::spawn("ChunkDistributionNetwork", async move {
        match inner_request_chunk(thread_local_client, url, ancestor_hash, shard_id).await {
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

async fn inner_request_chunk(
    client: reqwest::Client,
    url: String,
    prev_hash: CryptoHash,
    shard_id: String,
) -> anyhow::Result<PartialEncodedChunk> {
    let request =
        client.get(url).query(&[("prev_hash", prev_hash.to_string()), ("shard_id", shard_id)]);
    let response = request.send().await.and_then(|r| r.error_for_status())?;
    let chunk = PartialEncodedChunk::deserialize(&mut response.bytes().await?.as_ref())?;
    Ok(chunk)
}
