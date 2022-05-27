use actix::{Actor, Context, Handler, Recipient};
use near_chain::{Block, BlockHeader, Chain, ChainStoreAccess, Error};
use near_chain_configs::GenesisConfig;
use near_client::sync;
use near_network::types::{
    FullPeerInfo, NetworkClientMessages, NetworkInfo, NetworkRequests, NetworkResponses,
    PeerManagerMessageRequest, PeerManagerMessageResponse,
};
use near_network_primitives::types::{
    PartialEdgeInfo, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg, PeerInfo,
};
use near_performance_metrics::actix::run_later;
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::time::Clock;
use near_primitives::types::{BlockHeight, ShardId};
use std::collections::HashMap;
use std::time::Duration;

pub mod setup;

/// MockPeerManagerActor mocks PeerManagerActor and responds to messages from ClientActor.
/// Instead of sending these messages out to other peers, it simulates a network and reads
/// the needed block and chunk content from storage.
/// MockPeerManagerActor has the following responsibilities
/// - Responds to the requests sent from ClientActor, including
///     BlockRequest, BlockHeadersRequest and PartialEncodedChunkRequest
/// - Sends NetworkInfo to ClientActor periodically
/// - Simulates block production and sends the most "recent" block to ClientActor
pub struct MockPeerManagerActor {
    /// Client address for the node that we are testing
    client_addr: Recipient<NetworkClientMessages>,
    /// Access a pre-generated chain history from storage
    chain_history_access: ChainHistoryAccess,
    /// Current network state for the simulated network
    network_info: NetworkInfo,
    /// Block production speed for the network that we are simulating
    block_production_delay: Duration,
    /// Simulated network delay
    network_delay: Duration,
    /// The simulated peers will stop producing new blocks at this height
    target_height: BlockHeight,
}

impl MockPeerManagerActor {
    fn new(
        client_addr: Recipient<NetworkClientMessages>,
        genesis_config: &GenesisConfig,
        chain: Chain,
        peers_start_height: BlockHeight,
        target_height: BlockHeight,
        block_production_delay: Duration,
        network_delay: Duration,
    ) -> Self {
        // for now, we only simulate one peer
        // we will add more complicated network config in the future
        let peer = FullPeerInfo {
            peer_info: PeerInfo::random(),
            chain_info: near_network_primitives::types::PeerChainInfoV2 {
                genesis_id: GenesisId {
                    chain_id: genesis_config.chain_id.clone(),
                    hash: *chain.genesis().hash(),
                },
                height: peers_start_height,
                tracked_shards: (0..genesis_config.shard_layout.num_shards()).collect(),
                archival: false,
            },
            partial_edge_info: PartialEdgeInfo::default(),
        };
        let network_info = NetworkInfo {
            connected_peers: vec![peer.clone()],
            num_connected_peers: 1,
            peer_max_count: 1,
            highest_height_peers: vec![peer],
            sent_bytes_per_sec: 0,
            received_bytes_per_sec: 0,
            known_producers: vec![],
            peer_counter: 0,
        };
        Self {
            client_addr,
            chain_history_access: ChainHistoryAccess { chain, target_height },
            network_info,
            block_production_delay,
            network_delay,
            target_height,
        }
    }

    /// This function gets called periodically
    /// When it is called, it increments peer heights by 1 and sends the block at that height
    /// to ClientActor. In a way, it simulates peers that broadcast new blocks
    fn update_peers(&mut self, ctx: &mut Context<MockPeerManagerActor>) {
        let _response =
            self.client_addr.do_send(NetworkClientMessages::NetworkInfo(self.network_info.clone()));
        for peer in self.network_info.connected_peers.iter_mut() {
            let current_height = peer.chain_info.height;
            if current_height <= self.target_height {
                if let Ok(block) =
                    self.chain_history_access.retrieve_block_by_height(current_height)
                {
                    let _response = self.client_addr.do_send(NetworkClientMessages::Block(
                        block,
                        peer.peer_info.id.clone(),
                        false,
                    ));
                }
                peer.chain_info.height = current_height + 1;
            }
        }
        self.network_info.highest_height_peers = self.network_info.connected_peers.clone();
        near_performance_metrics::actix::run_later(
            ctx,
            self.block_production_delay,
            move |act, ctx| {
                act.update_peers(ctx);
            },
        );
    }
}

impl Actor for MockPeerManagerActor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        // Start syncing job.
        self.update_peers(ctx);
    }
}

impl Handler<PeerManagerMessageRequest> for MockPeerManagerActor {
    type Result = PeerManagerMessageResponse;

    fn handle(&mut self, msg: PeerManagerMessageRequest, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            PeerManagerMessageRequest::NetworkRequests(request) => match request {
                NetworkRequests::BlockRequest { hash, peer_id } => {
                    run_later(ctx, self.network_delay, move |act, _ctx| {
                        let block = act.chain_history_access.retrieve_block(&hash).unwrap();
                        let _response = act
                            .client_addr
                            .do_send(NetworkClientMessages::Block(block, peer_id, true));
                    });
                }
                NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                    run_later(ctx, self.network_delay, move |act, _ctx| {
                        let headers = act
                            .chain_history_access
                            .retrieve_block_headers(hashes.clone())
                            .unwrap();
                        let _response = act
                            .client_addr
                            .do_send(NetworkClientMessages::BlockHeaders(headers, peer_id));
                    });
                }
                NetworkRequests::PartialEncodedChunkRequest { request, .. } => {
                    run_later(ctx, self.network_delay, move |act, _ctx| {
                        let response = act
                            .chain_history_access
                            .retrieve_partial_encoded_chunk(&request)
                            .unwrap();
                        let _response = act.client_addr.do_send(
                            NetworkClientMessages::PartialEncodedChunkResponse(
                                response,
                                Clock::instant(),
                            ),
                        );
                    });
                }
                NetworkRequests::Block { .. } => {}
                NetworkRequests::StateRequestHeader { .. } => {
                    panic!(
                        "MockPeerManagerActor receives state sync request. \
                            It doesn't support state sync now. Try setting start_height \
                            and target_height to be at the same epoch to avoid state sync"
                    );
                }
                _ => {
                    panic!("MockPeerManagerActor receives unexpected message {:?}", request);
                }
            },
            _ => {
                panic!("MockPeerManagerActor receives unexpected message {:?}", msg);
            }
        }
        PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
    }
}

#[derive(actix::Message, Debug)]
#[rtype(result = "u64")]
pub struct GetChainTargetBlockHeight;

impl Handler<GetChainTargetBlockHeight> for MockPeerManagerActor {
    type Result = u64;

    fn handle(
        &mut self,
        _msg: GetChainTargetBlockHeight,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.target_height
    }
}

/// This class provides access a pre-generated chain history
struct ChainHistoryAccess {
    chain: Chain,
    target_height: BlockHeight,
}

impl ChainHistoryAccess {
    fn retrieve_block_headers(
        &mut self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<BlockHeader>, Error> {
        self.chain.retrieve_headers(hashes, sync::MAX_BLOCK_HEADERS, Some(self.target_height))
    }

    fn retrieve_block_by_height(&mut self, block_height: BlockHeight) -> Result<Block, Error> {
        self.chain.get_block_by_height(block_height).map(|b| b)
    }

    fn retrieve_block(&mut self, block_hash: &CryptoHash) -> Result<Block, Error> {
        self.chain.get_block(block_hash).map(|b| b)
    }

    fn retrieve_partial_encoded_chunk(
        &mut self,
        request: &PartialEncodedChunkRequestMsg,
    ) -> Result<PartialEncodedChunkResponseMsg, Error> {
        let num_total_parts = self.chain.runtime_adapter.num_total_parts();
        let partial_chunk = self.chain.mut_store().get_partial_chunk(&request.chunk_hash)?;
        let present_parts: HashMap<u64, _> =
            partial_chunk.parts().iter().map(|part| (part.part_ord, part)).collect();
        assert_eq!(
            present_parts.len(),
            num_total_parts,
            "chunk {:?} doesn't have all parts",
            request.chunk_hash
        );
        let parts: Vec<_> = request
            .part_ords
            .iter()
            .map(|ord| present_parts.get(ord).cloned().cloned().unwrap())
            .collect();

        // Same process for receipts as above for parts.
        let present_receipts: HashMap<ShardId, _> = partial_chunk
            .receipts()
            .iter()
            .map(|receipt| (receipt.1.to_shard_id, receipt))
            .collect();
        let receipts: Vec<_> = request
            .tracking_shards
            .iter()
            .map(|shard_id| present_receipts.get(shard_id).cloned().cloned().unwrap())
            .collect();

        Ok(PartialEncodedChunkResponseMsg {
            chunk_hash: request.chunk_hash.clone(),
            parts,
            receipts,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::ChainHistoryAccess;
    use near_chain::ChainGenesis;
    use near_chain::{Chain, RuntimeAdapter};
    use near_chain_configs::Genesis;
    use near_client::test_utils::TestEnv;
    use near_logger_utils::init_test_logger;
    use near_network_primitives::types::PartialEncodedChunkRequestMsg;
    use near_primitives::types::EpochId;
    use near_store::test_utils::create_test_store;
    use nearcore::config::GenesisExt;
    use std::path::Path;
    use std::sync::Arc;

    // build a TestEnv with one validator with 20 blocks of history, all empty
    fn setup_mock() -> (ChainHistoryAccess, TestEnv) {
        let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        let chain_genesis = ChainGenesis::from(&genesis);
        let runtimes = vec![Arc::new(nearcore::NightshadeRuntime::test(
            Path::new("../../../.."),
            create_test_store(),
            &genesis,
        )) as Arc<dyn RuntimeAdapter>];
        let mut env = TestEnv::builder(chain_genesis.clone())
            .validator_seats(1)
            .runtime_adapters(runtimes.clone())
            .build();
        for i in 0..20 {
            env.produce_block(0, i + 1);
        }

        let chain = Chain::new(
            runtimes[0].clone(),
            &chain_genesis,
            env.clients[0].chain.doomslug_threshold_mode,
            true,
        )
        .unwrap();
        (ChainHistoryAccess { chain, target_height: 21 }, env)
    }

    #[test]
    fn test_chain_history_access() {
        init_test_logger();
        let (mut chain_history_access, env) = setup_mock();
        let blocks: Vec<_> =
            (1..21).map(|h| env.clients[0].chain.get_block_by_height(h).unwrap()).collect();

        for block in blocks.iter() {
            assert_eq!(&chain_history_access.retrieve_block(block.hash()).unwrap(), block);
        }

        for block in blocks.iter() {
            for chunk in block.chunks().iter() {
                if chunk.height_included() == block.header().height() {
                    let _partial_encoded_chunk_response = chain_history_access
                        .retrieve_partial_encoded_chunk(&PartialEncodedChunkRequestMsg {
                            chunk_hash: chunk.chunk_hash(),
                            part_ords: (0..env.clients[0].runtime_adapter.num_total_parts() as u64)
                                .collect(),
                            tracking_shards: (0..env.clients[0]
                                .runtime_adapter
                                .num_shards(&EpochId::default())
                                .unwrap())
                                .collect(),
                        })
                        .unwrap();
                }
            }
        }

        for block in blocks.iter() {
            // check the retrieve block headers work. We don't check the results here since
            // it is simply a wrapper around Chain::retrieve_block_headers
            chain_history_access.retrieve_block_headers(vec![*block.hash()]).unwrap();
        }
    }
}
