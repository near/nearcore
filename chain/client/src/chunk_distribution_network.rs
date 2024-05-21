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
    types::{EpochId, ShardId},
};
use std::fmt;
use tracing::error;

/// Basic interface for the chunk distribution network.
pub trait ChunkDistributionClient {
    type Error;
    type Response;

    /// Attempt to lookup a chunk from the chunk distribution network
    /// given the parent block hash (`prev_block_hash`) and the `shard_id`
    /// for that chunk.
    async fn lookup_chunk(
        &self,
        prev_block_hash: CryptoHash,
        shard_id: ShardId,
    ) -> Result<Option<PartialEncodedChunk>, Self::Error>;

    /// Publish a chunk to the chunk distribution network.
    async fn publish_chunk(
        &mut self,
        chunk: &PartialEncodedChunk,
    ) -> Result<Self::Response, Self::Error>;
}

/// Helper struct for the Chunk Distribution Network Feature.
#[derive(Debug, Clone)]
pub struct ChunkDistributionNetwork {
    client: reqwest::Client,
    config: ChunkDistributionNetworkConfig,
}

impl ChunkDistributionNetwork {
    pub fn from_config(config: &ClientConfig) -> Option<Self> {
        config
            .chunk_distribution_network
            .as_ref()
            .map(|c| Self { client: reqwest::Client::new(), config: c.clone() })
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled
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
    for BlockMissingChunks { prev_hash, missing_chunks } in blocks_missing_chunks {
        for chunk in missing_chunks {
            blocks_delay_tracker.mark_chunk_requested(&chunk);
            request_missing_chunk(client.clone(), chunk, shards_manager_adapter.clone(), prev_hash);
        }
    }

    for OrphanMissingChunks { missing_chunks, epoch_id, ancestor_hash } in orphans_missing_chunks {
        for chunk in missing_chunks {
            blocks_delay_tracker.mark_chunk_requested(&chunk);
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
    header: ShardChunkHeader,
    adapter: Sender<ShardsManagerRequestFromClient>,
    prev_hash: CryptoHash,
) where
    C: ChunkDistributionClient + 'static,
    C::Error: fmt::Debug,
{
    let shard_id = header.shard_id();
    near_performance_metrics::actix::spawn("ChunkDistributionNetwork", async move {
        match client.lookup_chunk(prev_hash, shard_id).await {
            Ok(Some(chunk)) => {
                adapter.send(ShardsManagerRequestFromClient::ProcessOrRequestChunk {
                    candidate_chunk: chunk,
                    request_header: header,
                    prev_hash,
                });
            }
            Ok(None) => {
                adapter.send(ShardsManagerRequestFromClient::RequestChunks {
                    chunks_to_request: vec![header],
                    prev_hash,
                });
            }
            Err(err) => {
                error!(target: "client", ?err, "Failed to find chunk via Chunk Distribution Network");
                adapter.send(ShardsManagerRequestFromClient::RequestChunks {
                    chunks_to_request: vec![header],
                    prev_hash,
                });
            }
        }
    });
}

fn request_orphan_chunk<C>(
    client: C,
    header: ShardChunkHeader,
    adapter: &Sender<ShardsManagerRequestFromClient>,
    epoch_id: EpochId,
    ancestor_hash: CryptoHash,
) where
    C: ChunkDistributionClient + 'static,
    C::Error: fmt::Debug,
{
    let shard_id = header.shard_id();
    let thread_local_shards_manager_adapter = adapter.clone();
    near_performance_metrics::actix::spawn("ChunkDistributionNetwork", async move {
        match client.lookup_chunk(ancestor_hash, shard_id).await {
            Ok(Some(chunk)) => {
                thread_local_shards_manager_adapter.send(
                    ShardsManagerRequestFromClient::ProcessOrRequestChunkForOrphan {
                        candidate_chunk: chunk,
                        request_header: header,
                        epoch_id,
                        ancestor_hash,
                    },
                );
            }
            Ok(None) => {
                thread_local_shards_manager_adapter.send(
                    ShardsManagerRequestFromClient::RequestChunksForOrphan {
                        chunks_to_request: vec![header],
                        epoch_id,
                        ancestor_hash,
                    },
                );
            }
            Err(err) => {
                error!(target: "client", ?err, "Failed to find chunk via Chunk Distribution Network");
                thread_local_shards_manager_adapter.send(
                    ShardsManagerRequestFromClient::RequestChunksForOrphan {
                        chunks_to_request: vec![header],
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
    ) -> Result<Option<PartialEncodedChunk>, Self::Error> {
        let url = &self.config.uris.get;
        let request = self
            .client
            .get(url)
            .query(&[("prev_hash", prev_hash.to_string()), ("shard_id", shard_id.to_string())]);
        let response = request.send().await?.error_for_status()?;
        let chunk = PartialEncodedChunk::deserialize(&mut response.bytes().await?.as_ref()).ok();
        Ok(chunk)
    }

    async fn publish_chunk(
        &mut self,
        chunk: &PartialEncodedChunk,
    ) -> Result<Self::Response, Self::Error> {
        let prev_hash = chunk.prev_block();
        let bytes = borsh::to_vec(chunk)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use near_async::{
        messaging::{CanSend, IntoSender},
        time::Clock,
    };
    use near_primitives::{
        congestion_info::CongestionInfo,
        hash::hash,
        sharding::{
            PartialEncodedChunkV2, ShardChunkHeaderInner, ShardChunkHeaderInnerV3,
            ShardChunkHeaderV3,
        },
        validator_signer::EmptyValidatorSigner,
    };
    use std::{collections::HashMap, convert::Infallible, future::Future};
    use tokio::sync::mpsc;

    #[test]
    fn test_request_chunks() {
        let (mock_sender, mut message_receiver) = mpsc::unbounded_channel();
        let mut client = MockClient::default();
        let missing_chunk = mock_shard_chunk(0, 0);
        let mut blocks_delay_tracker = BlocksDelayTracker::new(Clock::real());
        let shards_manager = MockSender::new(mock_sender);
        let shards_manager_adapter = shards_manager.into_sender();

        let system = actix::System::new();

        // Empty input is a noop
        system.block_on(async {
            request_missing_chunks(
                Vec::new(),
                Vec::new(),
                client.clone(),
                &mut blocks_delay_tracker,
                &shards_manager_adapter,
            );
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => (),
                _ = message_receiver.recv() => panic!("Unexpected message"),
            }
        });

        // Block missing chunks unknown to the client are requested by shard manager
        let blocks_missing_chunks = vec![BlockMissingChunks {
            prev_hash: *missing_chunk.prev_block(),
            missing_chunks: vec![missing_chunk.cloned_header()],
        }];
        system.block_on(async {
            request_missing_chunks(
                blocks_missing_chunks,
                Vec::new(),
                client.clone(),
                &mut blocks_delay_tracker,
                &shards_manager_adapter,
            );
            let message = message_receiver.recv().await.unwrap();
            assert_eq!(
                message,
                ShardsManagerRequestFromClient::RequestChunks {
                    chunks_to_request: vec![missing_chunk.cloned_header()],
                    prev_hash: *missing_chunk.prev_block()
                }
            );
            assert_eq!(message_receiver.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
        });

        // Orphan missing chunks unknown to the client are requested by shard manager
        let orphans_missing_chunks = vec![OrphanMissingChunks {
            missing_chunks: vec![missing_chunk.cloned_header()],
            epoch_id: EpochId::default(),
            ancestor_hash: *missing_chunk.prev_block(),
        }];
        system.block_on(async {
            request_missing_chunks(
                Vec::new(),
                orphans_missing_chunks,
                client.clone(),
                &mut blocks_delay_tracker,
                &shards_manager_adapter,
            );
            let message = message_receiver.recv().await.unwrap();
            assert_eq!(
                message,
                ShardsManagerRequestFromClient::RequestChunksForOrphan {
                    chunks_to_request: vec![missing_chunk.cloned_header()],
                    epoch_id: EpochId::default(),
                    ancestor_hash: *missing_chunk.prev_block()
                }
            );
            assert_eq!(message_receiver.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
        });

        // When chunks are known by the client, the shards manager
        // is told to process the chunk directly
        let known_chunk_1 = mock_shard_chunk(1, 0);
        let known_chunk_2 = mock_shard_chunk(2, 0);
        client.publish_chunk(&known_chunk_1).now_or_never();
        client.publish_chunk(&known_chunk_2).now_or_never();
        let blocks_missing_chunks = vec![BlockMissingChunks {
            prev_hash: *known_chunk_1.prev_block(),
            missing_chunks: vec![known_chunk_1.cloned_header()],
        }];
        let orphans_missing_chunks = vec![OrphanMissingChunks {
            missing_chunks: vec![known_chunk_2.cloned_header()],
            epoch_id: EpochId::default(),
            ancestor_hash: *known_chunk_2.prev_block(),
        }];
        system.block_on(async {
            request_missing_chunks(
                blocks_missing_chunks,
                orphans_missing_chunks,
                client,
                &mut blocks_delay_tracker,
                &shards_manager_adapter,
            );
            let message = message_receiver.recv().await.unwrap();
            assert_eq!(
                message,
                ShardsManagerRequestFromClient::ProcessOrRequestChunk {
                    candidate_chunk: known_chunk_1.clone(),
                    request_header: known_chunk_1.cloned_header(),
                    prev_hash: *known_chunk_1.prev_block()
                }
            );
            let message = message_receiver.recv().await.unwrap();
            assert_eq!(
                message,
                ShardsManagerRequestFromClient::ProcessOrRequestChunkForOrphan {
                    candidate_chunk: known_chunk_2.clone(),
                    request_header: known_chunk_2.cloned_header(),
                    epoch_id: EpochId::default(),
                    ancestor_hash: *known_chunk_2.prev_block(),
                }
            );
            assert_eq!(message_receiver.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
        });

        // If the chunk distribution client is broken (returns errors to requests) then
        // the p2p network is used for requesting chunks.
        let blocks_missing_chunks = vec![BlockMissingChunks {
            prev_hash: *known_chunk_1.prev_block(),
            missing_chunks: vec![known_chunk_1.cloned_header()],
        }];
        let orphans_missing_chunks = vec![OrphanMissingChunks {
            missing_chunks: vec![known_chunk_2.cloned_header()],
            epoch_id: EpochId::default(),
            ancestor_hash: *known_chunk_2.prev_block(),
        }];
        system.block_on(async {
            request_missing_chunks(
                blocks_missing_chunks,
                orphans_missing_chunks,
                ErrorClient, // intentionally broken chunk distribution client
                &mut blocks_delay_tracker,
                &shards_manager_adapter,
            );
            let message = message_receiver.recv().await.unwrap();
            assert_eq!(
                message,
                ShardsManagerRequestFromClient::RequestChunks {
                    chunks_to_request: vec![known_chunk_1.cloned_header()],
                    prev_hash: *known_chunk_1.prev_block()
                }
            );
            let message = message_receiver.recv().await.unwrap();
            assert_eq!(
                message,
                ShardsManagerRequestFromClient::RequestChunksForOrphan {
                    chunks_to_request: vec![known_chunk_2.cloned_header()],
                    epoch_id: EpochId::default(),
                    ancestor_hash: *known_chunk_2.prev_block(),
                }
            );
            assert_eq!(message_receiver.try_recv().unwrap_err(), mpsc::error::TryRecvError::Empty);
        });
    }

    fn mock_shard_chunk(height: u64, shard_id: u64) -> PartialEncodedChunk {
        let prev_block_hash =
            hash(&[height.to_le_bytes().as_slice(), shard_id.to_le_bytes().as_slice()].concat());
        let mut mock_hashes = MockHashes::new(prev_block_hash);

        let signer = ValidatorSigner::EmptyValidatorSigner(EmptyValidatorSigner::default());
        let header_inner = ShardChunkHeaderInner::V3(ShardChunkHeaderInnerV3 {
            prev_block_hash,
            prev_state_root: mock_hashes.next().unwrap(),
            prev_outcome_root: mock_hashes.next().unwrap(),
            encoded_merkle_root: mock_hashes.next().unwrap(),
            encoded_length: 0,
            height_created: height,
            shard_id,
            prev_gas_used: 0,
            gas_limit: 0,
            prev_balance_burnt: 0,
            prev_outgoing_receipts_root: mock_hashes.next().unwrap(),
            tx_root: mock_hashes.next().unwrap(),
            prev_validator_proposals: Vec::new(),
            congestion_info: CongestionInfo::default(),
        });
        let header = ShardChunkHeaderV3::from_inner(header_inner, &signer);
        PartialEncodedChunk::V2(PartialEncodedChunkV2 {
            header: ShardChunkHeader::V3(header),
            parts: Vec::new(),
            prev_outgoing_receipts: Vec::new(),
        })
    }

    #[derive(Debug, Default, Clone)]
    struct MockClient {
        known_chunks: HashMap<(CryptoHash, ShardId), PartialEncodedChunk>,
    }
    impl ChunkDistributionClient for MockClient {
        type Error = Infallible;
        type Response = ();

        fn lookup_chunk(
            &self,
            prev_hash: CryptoHash,
            shard_id: ShardId,
        ) -> impl Future<Output = Result<Option<PartialEncodedChunk>, Self::Error>> {
            let chunk = self.known_chunks.get(&(prev_hash, shard_id)).cloned();
            futures::future::ready(Ok(chunk))
        }

        fn publish_chunk(
            &mut self,
            chunk: &PartialEncodedChunk,
        ) -> impl Future<Output = Result<Self::Response, Self::Error>> {
            let prev_hash = *chunk.prev_block();
            let shard_id = chunk.shard_id();
            self.known_chunks.insert((prev_hash, shard_id), chunk.clone());
            futures::future::ready(Ok(()))
        }
    }

    #[derive(Debug, Clone)]
    struct ErrorClient;
    impl ChunkDistributionClient for ErrorClient {
        type Error = ();
        type Response = Infallible;

        fn lookup_chunk(
            &self,
            _prev_hash: CryptoHash,
            _shard_id: ShardId,
        ) -> impl Future<Output = Result<Option<PartialEncodedChunk>, Self::Error>> {
            futures::future::ready(Err(()))
        }

        fn publish_chunk(
            &mut self,
            _chunk: &PartialEncodedChunk,
        ) -> impl Future<Output = Result<Self::Response, Self::Error>> {
            futures::future::ready(Err(()))
        }
    }

    #[derive(Debug)]
    struct MockSender<M> {
        inner: mpsc::UnboundedSender<M>,
    }
    impl<M> MockSender<M> {
        fn new(sender: mpsc::UnboundedSender<M>) -> Self {
            Self { inner: sender }
        }
    }
    impl<M: Sync + Send + 'static> CanSend<M> for MockSender<M> {
        fn send(&self, message: M) {
            self.inner.send(message).unwrap();
        }
    }
    impl<M> Clone for MockSender<M> {
        fn clone(&self) -> Self {
            Self { inner: self.inner.clone() }
        }
    }

    struct MockHashes {
        state: CryptoHash,
    }
    impl MockHashes {
        fn new(init: CryptoHash) -> Self {
            Self { state: init }
        }
    }
    impl Iterator for MockHashes {
        type Item = CryptoHash;
        fn next(&mut self) -> Option<Self::Item> {
            self.state = hash(self.state.as_ref());
            Some(self.state)
        }
    }
}
