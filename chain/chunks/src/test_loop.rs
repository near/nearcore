use std::{collections::HashMap, sync::Arc};

use near_async::time;
use near_async::{
    messaging::Sender,
    test_loop::event_handler::{interval, LoopEventHandler, LoopHandlerContext, TryIntoOrSelf},
};
use near_chain::{types::Tip, Chain};
use near_epoch_manager::{
    shard_tracker::{ShardTracker, TrackedConfig},
    test_utils::{record_block, setup_epoch_manager_with_block_and_chunk_producers},
    EpochManagerAdapter, EpochManagerHandle,
};
use near_network::{
    shards_manager::ShardsManagerRequestFromNetwork,
    test_loop::SupportsRoutingLookup,
    types::{NetworkRequests, PeerManagerMessageRequest},
};
use near_primitives::{
    hash::CryptoHash,
    merkle::{self, MerklePath},
    sharding::{
        EncodedShardChunk, PartialEncodedChunk, PartialEncodedChunkV2, ReceiptProof,
        ReedSolomonWrapper, ShardChunkHeader,
    },
    test_utils::create_test_signer,
    types::{AccountId, BlockHeight, BlockHeightDelta, MerkleHash, NumShards, ShardId},
    version::PROTOCOL_VERSION,
};
use near_store::Store;

use crate::{
    adapter::ShardsManagerRequestFromClient,
    logic::{cares_about_shard_this_or_next_epoch, make_outgoing_receipts_proofs},
    test_utils::{default_tip, tip},
    ShardsManager,
};

pub fn forward_client_request_to_shards_manager(
) -> LoopEventHandler<ShardsManager, ShardsManagerRequestFromClient> {
    LoopEventHandler::new_simple(|event, data: &mut ShardsManager| {
        data.handle_client_request(event);
    })
}

pub fn forward_network_request_to_shards_manager(
) -> LoopEventHandler<ShardsManager, ShardsManagerRequestFromNetwork> {
    LoopEventHandler::new_simple(|event, data: &mut ShardsManager| {
        data.handle_network_request(event);
    })
}

/// Routes network messages that are issued by ShardsManager to other instances
/// in a multi-instance test.
///
/// TODO: This logic should ideally not be duplicated from the real
/// PeerManagerActor and PeerActor.
pub fn route_shards_manager_network_messages<
    Data: SupportsRoutingLookup,
    Event: TryIntoOrSelf<PeerManagerMessageRequest>
        + From<PeerManagerMessageRequest>
        + From<ShardsManagerRequestFromNetwork>,
>(
    network_delay: time::Duration,
) -> LoopEventHandler<Data, (usize, Event)> {
    let mut route_back_lookup: HashMap<CryptoHash, usize> = HashMap::new();
    let mut next_hash: u64 = 0;
    LoopEventHandler::new(
        move |event: (usize, Event),
              data: &mut Data,
              context: &LoopHandlerContext<(usize, Event)>| {
            let (idx, event) = event;
            let message = event.try_into_or_self().map_err(|e| (idx, e.into()))?;
            match message {
                PeerManagerMessageRequest::NetworkRequests(request) => {
                    match request {
                        NetworkRequests::PartialEncodedChunkRequest { target, request, .. } => {
                            let target_idx = data.index_for_account(&target.account_id.unwrap());
                            let route_back = CryptoHash::hash_borsh(next_hash);
                            route_back_lookup.insert(route_back, idx);
                            next_hash += 1;
                            context.sender.send_with_delay(
                            (target_idx,
                            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                                partial_encoded_chunk_request: request,
                                route_back,
                            }.into()),
                            network_delay,
                        );
                            Ok(())
                        }
                        NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
                            let target_idx =
                                *route_back_lookup.get(&route_back).expect("Route back not found");
                            context.sender.send_with_delay(
                            (target_idx,
                            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                                partial_encoded_chunk_response: response,
                                received_time: context.clock.now().into(), // TODO: use clock
                            }.into()),
                            network_delay,
                        );
                            Ok(())
                        }
                        NetworkRequests::PartialEncodedChunkMessage {
                            account_id,
                            partial_encoded_chunk,
                        } => {
                            let target_idx = data.index_for_account(&account_id);
                            context.sender.send_with_delay(
                                (
                                    target_idx,
                                    ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                                        partial_encoded_chunk.into(),
                                    )
                                    .into(),
                                ),
                                network_delay,
                            );
                            Ok(())
                        }
                        NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
                            let target_idx = data.index_for_account(&account_id);
                            context.sender.send_with_delay(
                            (target_idx,
                            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(
                                forward,
                            ).into()),
                            network_delay,
                        );
                            Ok(())
                        }
                        other_message => Err((
                            idx,
                            PeerManagerMessageRequest::NetworkRequests(other_message).into(),
                        )),
                    }
                }
                message => Err((idx, message.into())),
            }
        },
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardsManagerResendChunkRequests;

/// Periodically call resend_chunk_requests.
pub fn periodically_resend_chunk_requests(
    every: time::Duration,
) -> LoopEventHandler<ShardsManager, ShardsManagerResendChunkRequests> {
    interval(every, ShardsManagerResendChunkRequests, |data: &mut ShardsManager| {
        data.resend_chunk_requests()
    })
}

/// A simple implementation of the chain side that interacts with
/// ShardsManager.
pub struct MockChainForShardsManager {
    pub account_id: AccountId,
    pub epoch_manager: Arc<EpochManagerHandle>,
    pub shard_tracker: ShardTracker,
    pub shards_manager: Sender<ShardsManagerRequestFromClient>,
    pub tip: Tip,
}

pub struct MockChainForShardsManagerConfig {
    pub account_id: AccountId,
    pub num_shards: NumShards,
    pub epoch_length: BlockHeightDelta,
    pub block_producers: Vec<AccountId>,
    pub chunk_only_producers: Vec<AccountId>,
    pub track_all_shards: bool,
    pub shards_manager: Sender<ShardsManagerRequestFromClient>,
}

impl MockChainForShardsManager {
    pub fn new(store: Store, config: MockChainForShardsManagerConfig) -> Self {
        let epoch_manager = Arc::new(
            setup_epoch_manager_with_block_and_chunk_producers(
                store,
                config.block_producers,
                config.chunk_only_producers,
                config.num_shards,
                config.epoch_length,
            )
            .into_handle(),
        );
        let tracking = if config.track_all_shards {
            TrackedConfig::AllShards
        } else {
            TrackedConfig::new_empty()
        };
        let shard_tracker = ShardTracker::new(tracking, epoch_manager.clone());
        Self {
            account_id: config.account_id,
            epoch_manager,
            shard_tracker,
            shards_manager: config.shards_manager,
            tip: default_tip(),
        }
    }

    /// Adds a new block to the chain. Automatically takes care of Epoch
    /// transitions, and automatically updates the tip, and notifies the
    /// ShardsManager about the new tip.
    pub fn record_block(&mut self, last: CryptoHash, height: BlockHeight) {
        record_block(
            &mut *self.epoch_manager.write(),
            self.tip.last_block_hash,
            last,
            height,
            vec![],
        );
        self.tip = tip(self.epoch_manager.as_ref(), last);
        self.shards_manager.send(ShardsManagerRequestFromClient::UpdateChainHeads {
            head: self.tip.clone(),
            header_head: self.tip.clone(),
        });
    }

    /// Makes a request to the ShardsManager to fetch this chunk.
    pub fn request_chunk_for_block(&mut self, chunk_header: ShardChunkHeader) {
        // TODO: this request and the next request are somewhat redundant, and the next
        // request does not work without the first one. We should consolidate the two
        // requests.
        self.shards_manager.send(ShardsManagerRequestFromClient::ProcessChunkHeaderFromBlock(
            chunk_header.clone(),
        ));
        if &self.tip.last_block_hash == chunk_header.prev_block_hash() {
            self.shards_manager.send(ShardsManagerRequestFromClient::RequestChunks {
                chunks_to_request: vec![chunk_header.clone()],
                prev_hash: *chunk_header.prev_block_hash(),
            });
        } else {
            self.shards_manager.send(ShardsManagerRequestFromClient::RequestChunksForOrphan {
                chunks_to_request: vec![chunk_header],
                epoch_id: self
                    .epoch_manager
                    .get_epoch_id_from_prev_block(&self.tip.last_block_hash)
                    .unwrap(),
                ancestor_hash: self.tip.last_block_hash,
            });
        }
    }

    /// Calculates the next chunk producer (for tip.height + 1)
    pub fn next_chunk_producer(&mut self, shard_id: ShardId) -> AccountId {
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(&self.tip.last_block_hash).unwrap();
        self.epoch_manager.get_chunk_producer(&epoch_id, self.tip.height + 1, shard_id).unwrap()
    }

    /// Produces the next chunk for the given shard, signed by the chunk
    /// producer who is supposed to produce it.
    pub fn produce_chunk_signed_by_chunk_producer(
        &mut self,
        shard_id: ShardId,
    ) -> TestChunkEncoder {
        let receipts = Vec::new();
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(&self.tip.last_block_hash).unwrap();
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let receipts_hashes = Chain::build_receipts_hashes(&receipts, &shard_layout);
        let (receipts_root, _) = merkle::merklize(&receipts_hashes);
        let chunk_producer = self.next_chunk_producer(shard_id);
        let signer = create_test_signer(chunk_producer.as_str());
        let data_parts = self.epoch_manager.num_data_parts();
        let parity_parts = self.epoch_manager.num_total_parts() - data_parts;
        let mut rs = ReedSolomonWrapper::new(data_parts, parity_parts);
        let (chunk, merkle_paths) = ShardsManager::create_encoded_shard_chunk(
            self.tip.last_block_hash,
            CryptoHash::default(),
            CryptoHash::default(),
            self.tip.height + 1,
            shard_id,
            0,
            1000,
            0,
            Vec::new(),
            Vec::new(),
            &receipts,
            receipts_root,
            MerkleHash::default(),
            &signer,
            &mut rs,
            PROTOCOL_VERSION,
        )
        .unwrap();
        let receipt_proofs =
            make_outgoing_receipts_proofs(&chunk.cloned_header(), &[], self.epoch_manager.as_ref())
                .unwrap()
                .collect();
        TestChunkEncoder::new(chunk, merkle_paths, receipt_proofs)
    }

    /// Produces the next chunk, asserting that we are the chunk producer.
    pub fn produce_chunk(&mut self, shard_id: ShardId) -> TestChunkEncoder {
        assert!(self.next_chunk_producer(shard_id) == self.account_id, "Cannot use produce_chunk if we are not the chunk producer; try produce_chunk_signed_by_chunk_producer.");
        self.produce_chunk_signed_by_chunk_producer(shard_id)
    }

    /// Distributes the produced chunk via the ShardsManager.
    pub fn distribute_chunk(&mut self, chunk: &TestChunkEncoder) {
        self.shards_manager.send(ShardsManagerRequestFromClient::DistributeEncodedChunk {
            encoded_chunk: chunk.encoded_chunk.clone(),
            merkle_paths: chunk.merkle_paths.clone(),
            outgoing_receipts: Vec::new(),
            partial_chunk: chunk.full_partial_chunk.clone(),
        });
    }

    /// Whether we care about this shard this or next epoch,
    /// as of the current tip.
    pub fn cares_about_shard_this_or_next_epoch(&self, shard_id: ShardId) -> bool {
        cares_about_shard_this_or_next_epoch(
            Some(&self.account_id),
            &self.tip.last_block_hash,
            shard_id,
            true,
            &self.shard_tracker,
        )
    }
}

/// A helper struct for encoding partial chunks, for a specific chunk.
pub struct TestChunkEncoder {
    encoded_chunk: EncodedShardChunk,
    full_partial_chunk: PartialEncodedChunk,
    merkle_paths: Vec<MerklePath>,
}

impl TestChunkEncoder {
    pub fn new(
        encoded_chunk: EncodedShardChunk,
        merkle_paths: Vec<MerklePath>,
        receipt_proofs: Vec<ReceiptProof>,
    ) -> Self {
        let all_part_ords =
            encoded_chunk.content().parts.iter().enumerate().map(|(i, _)| i as u64).collect();
        let full_partial_chunk = encoded_chunk.create_partial_encoded_chunk(
            all_part_ords,
            receipt_proofs,
            &merkle_paths,
        );
        Self { encoded_chunk, full_partial_chunk, merkle_paths }
    }

    pub fn part_ords(&self) -> Vec<u64> {
        self.full_partial_chunk.parts().iter().map(|part| part.part_ord).collect()
    }

    pub fn make_partial_encoded_chunk(
        &self,
        part_ords: &[u64],
        receipt_shards: &[ShardId],
    ) -> PartialEncodedChunk {
        let parts = part_ords
            .iter()
            .copied()
            .flat_map(|ord| {
                self.full_partial_chunk.parts().iter().find(|part| part.part_ord == ord)
            })
            .cloned()
            .collect();
        PartialEncodedChunk::V2(PartialEncodedChunkV2 {
            header: self.encoded_chunk.cloned_header(),
            parts,
            receipts: self
                .full_partial_chunk
                .receipts()
                .iter()
                .enumerate()
                .filter(|(i, _)| receipt_shards.contains(&(*i as ShardId)))
                .map(|(_, receipt)| receipt.clone())
                .collect(),
        })
    }

    pub fn header(&self) -> ShardChunkHeader {
        self.encoded_chunk.cloned_header()
    }
}
