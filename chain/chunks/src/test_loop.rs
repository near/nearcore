use crate::{
    adapter::ShardsManagerRequestFromClient,
    logic::{cares_about_shard_this_or_next_epoch, make_outgoing_receipts_proofs},
    shards_manager_actor::ShardsManagerActor,
    test_utils::{default_tip, tip},
};
use near_async::test_loop::delay_sender::DelaySender;
use near_async::test_loop::futures::TestLoopDelayedActionRunner;
use near_async::time::Clock;
use near_async::v2::{LoopData, LoopStream};
use near_async::{
    messaging::Sender,
    test_loop::event_handler::{LoopEventHandler, TryIntoOrSelf},
};
use near_async::{time, v2};
use near_chain::{types::Tip, Chain};
use near_epoch_manager::{
    shard_tracker::{ShardTracker, TrackedConfig},
    test_utils::{record_block, setup_epoch_manager_with_block_and_chunk_producers},
    EpochManagerAdapter, EpochManagerHandle,
};
use near_network::types::PeerManagerAdapterMessage;
use near_network::{
    shards_manager::ShardsManagerRequestFromNetwork,
    test_loop::SupportsRoutingLookup,
    types::{NetworkRequests, PeerManagerMessageRequest},
};
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::{
    hash::CryptoHash,
    merkle::{self, MerklePath},
    sharding::{
        EncodedShardChunk, PartialEncodedChunk, PartialEncodedChunkV2, ReceiptProof,
        ShardChunkHeader,
    },
    test_utils::create_test_signer,
    types::{AccountId, BlockHeight, BlockHeightDelta, MerkleHash, NumShards, ShardId},
    version::PROTOCOL_VERSION,
};
use near_store::Store;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::{collections::HashMap, sync::Arc};

pub fn forward_client_request_to_shards_manager(
) -> LoopEventHandler<ShardsManagerActor, ShardsManagerRequestFromClient> {
    LoopEventHandler::new_simple(|event, data: &mut ShardsManagerActor| {
        data.handle_client_request(event);
    })
}

pub fn forward_network_request_to_shards_manager(
) -> LoopEventHandler<ShardsManagerActor, ShardsManagerRequestFromNetwork> {
    LoopEventHandler::new_simple(|event, data: &mut ShardsManagerActor| {
        data.handle_network_request(event);
    })
}

#[derive(Clone)]
pub struct LoopShardsManagerActorBuilder {
    pub from_client_stream: LoopStream<ShardsManagerRequestFromClient>,
    pub from_network_stream: LoopStream<ShardsManagerRequestFromNetwork>,
    pub from_client: Sender<ShardsManagerRequestFromClient>,
    pub from_network: Sender<ShardsManagerRequestFromNetwork>,
}

pub fn loop_shards_manager_actor_builder(test: &mut v2::TestLoop) -> LoopShardsManagerActorBuilder {
    let from_client_stream = test.new_stream();
    let from_client = from_client_stream.sender();
    let from_network_stream = test.new_stream();
    let from_network = from_network_stream.sender();
    LoopShardsManagerActorBuilder {
        from_client_stream,
        from_network_stream,
        from_client,
        from_network,
    }
}

#[derive(Clone)]
pub struct LoopShardsManagerActor {
    pub actor: LoopData<ShardsManagerActor>,
    pub from_client: Sender<ShardsManagerRequestFromClient>,
    pub from_network: Sender<ShardsManagerRequestFromNetwork>,
    pub delayed_action_runner: TestLoopDelayedActionRunner<ShardsManagerActor>,
}

impl LoopShardsManagerActorBuilder {
    pub fn build(
        self,
        test: &mut v2::TestLoop,
        actor: ShardsManagerActor,
    ) -> LoopShardsManagerActor {
        let actor = test.add_data(actor);
        let delayed_action_runner = test.new_delayed_actions_runner(actor);
        self.from_client_stream.handle1_legacy(
            test,
            actor,
            forward_client_request_to_shards_manager(),
        );
        self.from_network_stream.handle1_legacy(
            test,
            actor,
            forward_network_request_to_shards_manager(),
        );
        LoopShardsManagerActor {
            actor,
            from_client: self.from_client,
            from_network: self.from_network,
            delayed_action_runner,
        }
    }
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
    sender: DelaySender<(usize, Event)>,
    clock: Clock,
    network_delay: time::Duration,
) -> LoopEventHandler<Data, (usize, Event)> {
    let mut route_back_lookup: HashMap<CryptoHash, usize> = HashMap::new();
    let mut next_hash: u64 = 0;
    LoopEventHandler::new(move |event: (usize, Event), data: &mut Data| {
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
                        sender.send_with_delay(
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
                        sender.send_with_delay(
                            (target_idx,
                            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                                partial_encoded_chunk_response: response,
                                received_time: clock.now().into(), // TODO: use clock
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
                        sender.send_with_delay(
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
                        sender.send_with_delay(
                            (
                                target_idx,
                                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(
                                    forward,
                                )
                                .into(),
                            ),
                            network_delay,
                        );
                        Ok(())
                    }
                    other_message => {
                        Err((idx, PeerManagerMessageRequest::NetworkRequests(other_message).into()))
                    }
                }
            }
            message => Err((idx, message.into())),
        }
    })
}

pub fn route_network_message_to_shards_manager(
    request: NetworkRequests,
    originator: &AccountId,
    senders: &HashMap<AccountId, DelaySender<ShardsManagerRequestFromNetwork>>,
    network_delay: time::Duration,
    clock: &Clock,
    route_back_lookup: &mut HashMap<CryptoHash, AccountId>,
) -> Result<(), NetworkRequests> {
    match request {
        NetworkRequests::PartialEncodedChunkRequest { target, request, .. } => {
            let target_account = target.account_id.as_ref().unwrap();
            let route_back = CryptoHash::hash_borsh(route_back_lookup.len() as u64);
            route_back_lookup.insert(route_back, originator.clone());
            senders[target_account].send_with_delay(
                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                    partial_encoded_chunk_request: request,
                    route_back,
                },
                network_delay,
            );
            Ok(())
        }
        NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
            let target_account = route_back_lookup.get(&route_back).expect("Route back not found");
            senders[target_account].send_with_delay(
                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                    partial_encoded_chunk_response: response,
                    received_time: clock.now().into(), // TODO: use clock
                }
                .into(),
                network_delay,
            );
            Ok(())
        }
        NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
            senders[&account_id].send_with_delay(
                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                    partial_encoded_chunk.into(),
                )
                .into(),
                network_delay,
            );
            Ok(())
        }
        NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
            senders[&account_id].send_with_delay(
                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(forward).into(),
                network_delay,
            );
            Ok(())
        }
        other_message => Err(other_message),
    }
}

pub fn handle_shards_manager_network_routing(
    stream: &LoopStream<PeerManagerAdapterMessage>,
    originator: &AccountId,
    test: &mut v2::TestLoop,
    senders: &HashMap<AccountId, DelaySender<ShardsManagerRequestFromNetwork>>,
    network_delay: time::Duration,
    route_back_lookup: LoopData<HashMap<CryptoHash, AccountId>>,
) {
    let clock = test.clock();
    let originator = originator.clone();
    let senders = senders.clone();
    stream.handle1(test, route_back_lookup, move |msg, route_back_lookup| {
        let PeerManagerAdapterMessage::_request_sender(request) = msg else { return Err(msg) };
        let PeerManagerMessageRequest::NetworkRequests(request) = request else {
            return Err(PeerManagerAdapterMessage::_request_sender(request));
        };
        match route_network_message_to_shards_manager(
            request,
            &originator,
            &senders,
            network_delay,
            &clock,
            route_back_lookup,
        ) {
            Ok(()) => Ok(()),
            Err(request) => Err(PeerManagerAdapterMessage::_request_sender(
                PeerManagerMessageRequest::NetworkRequests(request),
            )),
        }
    });
}

// NOTE: this is no longer needed for TestLoop, but some other non-TestLoop tests depend on it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardsManagerResendChunkRequests;

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
        let rs = ReedSolomon::new(data_parts, parity_parts).unwrap();
        let (chunk, merkle_paths) = ShardsManagerActor::create_encoded_shard_chunk(
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
            CongestionInfo::default(),
            &signer,
            &rs,
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
            prev_outgoing_receipts: self
                .full_partial_chunk
                .prev_outgoing_receipts()
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
