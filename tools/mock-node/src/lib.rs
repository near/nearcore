//! Implements `ChainHistoryAccess` and `MockPeerManagerActor`, which is the main
//! components of the mock network.

use actix::{Actor, Context, Handler};
use anyhow::{anyhow, Context as AnyhowContext};
use near_chain::{Block, BlockHeader, Chain, ChainStoreAccess, Error};
use near_chain_configs::GenesisConfig;
use near_client::sync::header::MAX_BLOCK_HEADERS;
use near_network::time;
use near_network::types::{
    BlockInfo, ConnectedPeerInfo, FullPeerInfo, NetworkInfo, NetworkRequests, NetworkResponses,
    PeerManagerMessageRequest, PeerManagerMessageResponse, PeerType, SetChainInfo,
};
use near_network::types::{
    PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg, PeerInfo,
};
use near_o11y::{handler_debug_span, OpenTelemetrySpanExt, WithSpanContext};
use near_performance_metrics::actix::run_later;
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::time::Clock;
use near_primitives::types::{BlockHeight, ShardId};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub mod setup;

// For now this is a simple struct with one field just to leave the door
// open for adding stuff and/or having different configs for different message types later.
#[derive(Clone, Debug, Deserialize)]
pub struct MockIncomingRequestConfig {
    // How long we wait between sending each incoming request
    interval: Duration,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MockIncomingRequestsConfig {
    // Options for sending unrequested blocks
    block: Option<MockIncomingRequestConfig>,
    // Options for sending chunk part requests
    chunk_request: Option<MockIncomingRequestConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MockNetworkConfig {
    #[serde(default = "default_delay")]
    // How long we'll wait until sending replies to the client
    pub response_delay: Duration,
    pub incoming_requests: Option<MockIncomingRequestsConfig>,
}

impl MockNetworkConfig {
    pub fn with_delay(response_delay: Duration) -> Self {
        let mut ret = Self::default();
        ret.response_delay = response_delay;
        ret
    }

    pub fn from_file<P: AsRef<Path>>(path: &P) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&s)?)
    }
}

pub const MOCK_DEFAULT_NETWORK_DELAY: Duration = Duration::from_millis(100);

fn default_delay() -> Duration {
    MOCK_DEFAULT_NETWORK_DELAY
}

impl Default for MockNetworkConfig {
    fn default() -> Self {
        Self { response_delay: default_delay(), incoming_requests: None }
    }
}

#[derive(Debug)]
// Info related to unrequested messages we'll send to the client
struct IncomingRequests {
    block: Option<(Duration, Block)>,
    chunk_request: Option<(Duration, PartialEncodedChunkRequestMsg)>,
}

// get some chunk hash to serve as the source of unrequested incoming chunks.
// For now we find the first chunk hash we know about starting from the height the client will start at.
// The lower the height, the better, so that the client will actually do some work on these
// requests instead of just seeing that the chunk hash is unknown.
fn retrieve_starting_chunk_hash(
    chain: &mut Chain,
    client_start_height: BlockHeight,
    target_height: BlockHeight,
) -> anyhow::Result<ChunkHash> {
    let mut last_err = None;
    for height in client_start_height..target_height + 1 {
        match chain
            .store()
            .get_block_hash_by_height(height)
            .and_then(|hash| chain.store().get_block(&hash))
            .map(|block| block.chunks().iter().next().unwrap().chunk_hash())
        {
            Ok(hash) => return Ok(hash),
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    match last_err {
        Some(e) => Err(e)
            .with_context(|| format!("Last error (retrieving chunk hash @ #{})", target_height)),
        None => Err(anyhow!("given target_height is not after the client start height?")),
    }
}

// get some block to serve as the source of unrequested incoming blocks.
fn retrieve_incoming_block(
    chain: &mut Chain,
    client_start_height: BlockHeight,
    target_height: BlockHeight,
) -> anyhow::Result<Block> {
    let mut last_err = None;
    for height in client_start_height..target_height + 1 {
        match chain.get_block_by_height(height) {
            Ok(b) => return Ok(b),
            Err(e) => {
                last_err = Some(e);
            }
        }
    }
    match last_err {
        Some(e) => {
            Err(e).with_context(|| format!("Last error (retrieving block #{})", target_height))
        }
        None => Err(anyhow!("given target_height is not after the client start height?")),
    }
}

impl IncomingRequests {
    fn new(
        config: &Option<MockIncomingRequestsConfig>,
        chain: &mut Chain,
        client_start_height: BlockHeight,
        target_height: BlockHeight,
    ) -> Self {
        let mut block = None;
        let mut chunk_request = None;

        if let Some(config) = config {
            if let Some(block_config) = &config.block {
                match retrieve_incoming_block(chain, client_start_height, target_height) {
                    Ok(b) => {
                        block = Some((block_config.interval, b));
                    }
                    Err(e) => {
                        tracing::error!("Can't retrieve block suitable for mock messages: {:?}", e);
                    }
                };
            }
            if let Some(chunk_request_config) = &config.chunk_request {
                match retrieve_starting_chunk_hash(chain, client_start_height, target_height) {
                    Ok(chunk_hash) => {
                        chunk_request = Some((
                            chunk_request_config.interval,
                            PartialEncodedChunkRequestMsg {
                                chunk_hash,
                                part_ords: vec![0],
                                tracking_shards: std::iter::once(0).collect::<HashSet<_>>(),
                            },
                        ));
                    }
                    Err(e) => {
                        tracing::error!(
                            "Can't construct chunk part request suitable for mock messages: {:?}",
                            e
                        );
                    }
                };
            }
        }

        Self { block, chunk_request }
    }
}

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
    client: Arc<dyn near_network::client::Client>,
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
    incoming_requests: IncomingRequests,
}

impl MockPeerManagerActor {
    fn new(
        client: Arc<dyn near_network::client::Client>,
        genesis_config: &GenesisConfig,
        mut chain: Chain,
        client_start_height: BlockHeight,
        network_start_height: BlockHeight,
        target_height: BlockHeight,
        block_production_delay: Duration,
        network_config: &MockNetworkConfig,
    ) -> Self {
        let start_block_hash = chain.get_block_hash_by_height(network_start_height).unwrap();
        // for now, we only simulate one peer
        // we will add more complicated network config in the future
        let peer = FullPeerInfo {
            peer_info: PeerInfo::random(),
            chain_info: near_network::types::PeerChainInfo {
                genesis_id: GenesisId {
                    chain_id: genesis_config.chain_id.clone(),
                    hash: *chain.genesis().hash(),
                },
                tracked_shards: (0..genesis_config.shard_layout.num_shards()).collect(),
                archival: false,
                last_block: Some(BlockInfo {
                    height: network_start_height,
                    hash: start_block_hash,
                }),
            },
        };
        let network_info = NetworkInfo {
            connected_peers: vec![ConnectedPeerInfo {
                full_peer_info: peer.clone(),
                received_bytes_per_sec: 0,
                sent_bytes_per_sec: 0,
                last_time_peer_requested: time::Instant::now(),
                last_time_received_message: time::Instant::now(),
                connection_established_time: time::Instant::now(),
                peer_type: PeerType::Outbound,
                nonce: 1,
            }],
            num_connected_peers: 1,
            peer_max_count: 1,
            highest_height_peers: vec![<FullPeerInfo as Into<Option<_>>>::into(peer).unwrap()],
            sent_bytes_per_sec: 0,
            received_bytes_per_sec: 0,
            known_producers: vec![],
            tier1_connections: vec![],
            tier1_accounts_data: vec![],
        };
        let incoming_requests = IncomingRequests::new(
            &network_config.incoming_requests,
            &mut chain,
            client_start_height,
            target_height,
        );
        Self {
            client,
            chain_history_access: ChainHistoryAccess { chain, target_height },
            network_info,
            block_production_delay,
            network_delay: network_config.response_delay,
            target_height,
            incoming_requests,
        }
    }

    /// This function gets called periodically
    /// When it is called, it increments peer heights by 1 and sends the block at that height
    /// to ClientActor. In a way, it simulates peers that broadcast new blocks
    fn update_peers(&mut self, ctx: &mut Context<MockPeerManagerActor>) {
        actix::spawn({
            let client = self.client.clone();
            let info = self.network_info.clone();
            async move { client.network_info(info).await }
        });
        for connected_peer in self.network_info.connected_peers.iter_mut() {
            let peer = &mut connected_peer.full_peer_info;
            let current_height = peer.chain_info.last_block.unwrap().height;
            if current_height <= self.target_height {
                if let Ok(block) =
                    self.chain_history_access.retrieve_block_by_height(current_height)
                {
                    actix::spawn({
                        let client = self.client.clone();
                        let peer_id = peer.peer_info.id.clone();
                        async move { client.block(block, peer_id, false).await }
                    });
                }
                let next_height = current_height + 1;
                while next_height <= self.target_height {
                    if let Ok(next_block) =
                        self.chain_history_access.retrieve_block_by_height(next_height)
                    {
                        peer.chain_info.last_block =
                            Some(BlockInfo { height: next_height, hash: *next_block.hash() });
                        break;
                    }
                }
            }
        }
        self.network_info.highest_height_peers = self
            .network_info
            .connected_peers
            .iter()
            .filter_map(|it| it.full_peer_info.clone().into())
            .collect();
        near_performance_metrics::actix::run_later(
            ctx,
            self.block_production_delay,
            move |act, ctx| {
                act.update_peers(ctx);
            },
        );
    }

    fn send_unrequested_block(&mut self, ctx: &mut Context<MockPeerManagerActor>) {
        if let Some((interval, block)) = &self.incoming_requests.block {
            actix::spawn({
                let client = self.client.clone();
                let block = block.clone();
                let peer_id =
                    self.network_info.connected_peers[0].full_peer_info.peer_info.id.clone();
                async move { client.block(block, peer_id, false).await }
            });

            run_later(ctx, *interval, move |act, ctx| {
                act.send_unrequested_block(ctx);
            });
        }
    }

    fn send_chunk_request(&mut self, ctx: &mut Context<MockPeerManagerActor>) {
        if let Some((interval, request)) = &self.incoming_requests.chunk_request {
            actix::spawn({
                let client = self.client.clone();
                let request = request.clone();
                async move {
                    client
                        .partial_encoded_chunk_request(
                            request.clone(),
                            // this can just be nonsense since the PeerManager is mocked out anyway. If/when we update the mock node
                            // to exercise the PeerManager code as well, then this won't matter anyway since the mock code won't be
                            // responsible for it.
                            CryptoHash::default(),
                        )
                        .await
                }
            });

            run_later(ctx, *interval, move |act, ctx| {
                act.send_chunk_request(ctx);
            });
        }
    }

    fn send_incoming_requests(&mut self, ctx: &mut Context<MockPeerManagerActor>) {
        self.send_unrequested_block(ctx);
        self.send_chunk_request(ctx);
    }
}

impl Actor for MockPeerManagerActor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        // Start syncing job.
        self.update_peers(ctx);

        self.send_incoming_requests(ctx);
    }
}

impl Handler<WithSpanContext<SetChainInfo>> for MockPeerManagerActor {
    type Result = ();

    fn handle(&mut self, _msg: WithSpanContext<SetChainInfo>, _ctx: &mut Self::Context) {}
}

impl Handler<WithSpanContext<PeerManagerMessageRequest>> for MockPeerManagerActor {
    type Result = PeerManagerMessageResponse;

    fn handle(
        &mut self,
        msg: WithSpanContext<PeerManagerMessageRequest>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        let (_span, msg) = handler_debug_span!(target: "mock-node", msg);
        match msg {
            PeerManagerMessageRequest::NetworkRequests(request) => match request {
                NetworkRequests::BlockRequest { hash, peer_id } => {
                    run_later(ctx, self.network_delay, move |act, _ctx| {
                        let block = act.chain_history_access.retrieve_block(&hash).unwrap();
                        actix::spawn({
                            let client = act.client.clone();
                            async move { client.block(block, peer_id, true).await }
                        });
                    });
                }
                NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                    run_later(ctx, self.network_delay, move |act, _ctx| {
                        let headers = act
                            .chain_history_access
                            .retrieve_block_headers(hashes.clone())
                            .unwrap();
                        actix::spawn({
                            let client = act.client.clone();
                            async move { client.block_headers(headers, peer_id).await }
                        });
                    });
                }
                NetworkRequests::PartialEncodedChunkRequest { request, .. } => {
                    run_later(ctx, self.network_delay, move |act, _ctx| {
                        let response = act
                            .chain_history_access
                            .retrieve_partial_encoded_chunk(&request)
                            .unwrap();
                        actix::spawn({
                            let client = act.client.clone();
                            async move {
                                client
                                    .partial_encoded_chunk_response(
                                        response,
                                        Clock::instant().into(),
                                    )
                                    .await
                            }
                        });
                    });
                }
                NetworkRequests::PartialEncodedChunkResponse { .. } => {}
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
        self.chain.retrieve_headers(hashes, MAX_BLOCK_HEADERS, Some(self.target_height))
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
    use near_network::types::PartialEncodedChunkRequestMsg;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::types::EpochId;
    use near_store::test_utils::create_test_store;
    use nearcore::config::GenesisExt;
    use std::path::Path;
    use std::sync::Arc;

    // build a TestEnv with one validator with 20 blocks of history, all empty
    fn setup_mock() -> (ChainHistoryAccess, TestEnv) {
        let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        let chain_genesis = ChainGenesis::new(&genesis);
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

        // Create view client chain to retrieve and check chain data in the test.
        let chain = Chain::new_for_view_client(
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
