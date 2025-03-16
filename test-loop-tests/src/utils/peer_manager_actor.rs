use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use itertools::Itertools;
use near_async::actix::ActixResult;
use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::{Actor, AsyncSender, CanSend, Handler, SendAsync, Sender};
use near_async::time::Clock;
use near_async::{MultiSend, MultiSenderFrom};
use near_client::{BlockApproval, BlockResponse};
use near_network::client::{
    BlockHeadersRequest, BlockHeadersResponse, BlockRequest, ChunkEndorsementMessage,
    EpochSyncRequestMessage, EpochSyncResponseMessage, OptimisticBlockMessage, ProcessTxRequest,
    ProcessTxResponse,
};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::state_witness::{
    ChunkContractAccessesMessage, ChunkStateWitnessAckMessage, ContractCodeRequestMessage,
    ContractCodeResponseMessage, PartialEncodedContractDeploysMessage,
    PartialEncodedStateWitnessForwardMessage, PartialEncodedStateWitnessMessage,
    PartialWitnessSenderForNetwork,
};
use near_network::types::{
    NetworkRequests, NetworkResponses, PeerManagerMessageRequest, PeerManagerMessageResponse,
    SetChainInfo, StateSyncEvent, Tier3Request,
};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;

/// Subset of ClientSenderForNetwork required for the TestLoop network.
/// We skip over the message handlers from view client.
#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ClientSenderForTestLoopNetwork {
    pub block: AsyncSender<BlockResponse, ()>,
    pub block_headers: AsyncSender<BlockHeadersResponse, ActixResult<BlockHeadersResponse>>,
    pub block_approval: AsyncSender<BlockApproval, ()>,
    pub chunk_endorsement: AsyncSender<ChunkEndorsementMessage, ()>,
    pub epoch_sync_request: Sender<EpochSyncRequestMessage>,
    pub epoch_sync_response: Sender<EpochSyncResponseMessage>,
    pub optimistic_block_receiver: Sender<OptimisticBlockMessage>,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct TxRequestHandleSenderForTestLoopNetwork {
    pub transaction: AsyncSender<ProcessTxRequest, ProcessTxResponse>,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ViewClientSenderForTestLoopNetwork {
    pub block_headers_request: AsyncSender<BlockHeadersRequest, ActixResult<BlockHeadersRequest>>,
    pub block_request: AsyncSender<BlockRequest, ActixResult<BlockRequest>>,
}

type NetworkRequestHandler = Box<dyn Fn(NetworkRequests) -> Option<NetworkRequests>>;

/// A custom actor for the TestLoop framework that can be used to send network messages across clients
/// in a multi-node test.
///
/// This actor has a set of handlers to handle PeerManagerMessageRequest messages. We have a set of
/// default handlers that handle messages sent to client, partial_witness actor, and shards_manager.
/// It is possible to override these handlers by registering a new handler using the
/// `register_override_handler()` method.
///
/// The signature of the handler is `dyn Fn(NetworkRequests) -> Option<NetworkRequests>`.
/// If the handler returns None, it means that the message was handled and no further processing is
/// required. If the handler returns Some(request), it means that the message was not handled and
/// the request should be passed to the next handler in the chain.
///
/// It's possible for a handler to modify the data in request and return it. This can be useful for
/// simulating things like malicious actors where we can modify the data in the request.
///
/// In case no handler is able to handle the request, the actor will panic.
///
/// NOTE: To make the override functionality work with the default handlers, the handlers are tried in
/// reverse order.
///
/// Examples of custom handlers
/// - Override handler to skip sending messages to or from a specific client.
/// - Override handler to simulate more network delays.
/// - Override handler to modify data and simulate malicious behavior.
pub struct TestLoopPeerManagerActor {
    handlers: Vec<NetworkRequestHandler>,
}

impl Actor for TestLoopPeerManagerActor {}

impl TestLoopPeerManagerActor {
    /// Create a new TestLoopPeerManagerActor with default handlers for client, partial_witness, and shards_manager.
    /// Note that we should be able to access the senders for these actors from the data type.
    pub fn new(
        clock: Clock,
        account_id: &AccountId,
        shared_state: &TestLoopNetworkSharedState,
        future_spawner: Arc<dyn FutureSpawner>,
    ) -> Self {
        let handlers = vec![
            network_message_to_client_handler(&account_id, shared_state.clone()),
            network_message_to_view_client_handler(
                account_id.clone(),
                shared_state.clone(),
                future_spawner,
            ),
            network_message_to_partial_witness_handler(&account_id, shared_state.clone()),
            network_message_to_shards_manager_handler(clock, &account_id, shared_state.clone()),
            network_message_to_state_snapshot_handler(),
        ];
        Self { handlers }
    }

    /// Register a new handler to override the default handlers.
    pub fn register_override_handler(&mut self, handler: NetworkRequestHandler) {
        // We add the handler to the end of the list and while processing the request, we iterate
        // over the handlers in reverse order.
        self.handlers.push(handler);
    }
}

/// Shared state across all the network actors. It handles the mapping between AccountId,
/// PeerId, and the route back CryptoHash, so that individual network actors can do
/// routing.
#[derive(Clone)]
pub struct TestLoopNetworkSharedState(Arc<Mutex<TestLoopNetworkSharedStateInner>>);

struct TestLoopNetworkSharedStateInner {
    account_to_peer_id: HashMap<AccountId, PeerId>,
    senders: HashMap<PeerId, Arc<OneClientSenders>>,
    route_back: HashMap<CryptoHash, PeerId>,
}

/// Senders available for the networking layer, for one node in the test loop.
struct OneClientSenders {
    client_sender: ClientSenderForTestLoopNetwork,
    view_client_sender: ViewClientSenderForTestLoopNetwork,
    tx_processor_sender: TxRequestHandleSenderForTestLoopNetwork,
    partial_witness_sender: PartialWitnessSenderForNetwork,
    shards_manager_sender: Sender<ShardsManagerRequestFromNetwork>,
}

impl TestLoopNetworkSharedState {
    pub fn new() -> Self {
        let inner = TestLoopNetworkSharedStateInner {
            account_to_peer_id: HashMap::new(),
            senders: HashMap::new(),
            route_back: HashMap::new(),
        };
        Self(Arc::new(Mutex::new(inner)))
    }

    pub fn add_client<'a, D>(&self, data: &'a D)
    where
        AccountId: From<&'a D>,
        PeerId: From<&'a D>,
        ClientSenderForTestLoopNetwork: From<&'a D>,
        ViewClientSenderForTestLoopNetwork: From<&'a D>,
        TxRequestHandleSenderForTestLoopNetwork: From<&'a D>,
        PartialWitnessSenderForNetwork: From<&'a D>,
        Sender<ShardsManagerRequestFromNetwork>: From<&'a D>,
    {
        let account_id = AccountId::from(data);
        let peer_id = PeerId::from(data);

        let mut guard = self.0.lock().unwrap();
        guard.account_to_peer_id.insert(account_id, peer_id.clone());
        guard.senders.insert(
            peer_id,
            Arc::new(OneClientSenders {
                client_sender: ClientSenderForTestLoopNetwork::from(data),
                view_client_sender: ViewClientSenderForTestLoopNetwork::from(data),
                tx_processor_sender: TxRequestHandleSenderForTestLoopNetwork::from(data),
                partial_witness_sender: PartialWitnessSenderForNetwork::from(data),
                shards_manager_sender: Sender::<ShardsManagerRequestFromNetwork>::from(data),
            }),
        );
    }

    fn account_to_peer_id(&self, account_id: &AccountId) -> PeerId {
        let guard = self.0.lock().unwrap();
        guard.account_to_peer_id.get(account_id).unwrap().clone()
    }

    fn senders_for_account(&self, account_id: &AccountId) -> Arc<OneClientSenders> {
        let guard = self.0.lock().unwrap();
        guard.senders.get(&guard.account_to_peer_id[account_id]).unwrap().clone()
    }

    fn senders_for_peer(&self, peer_id: &PeerId) -> Arc<OneClientSenders> {
        let guard = self.0.lock().unwrap();
        guard.senders.get(peer_id).unwrap().clone()
    }

    fn generate_route_back(&self, peer_id: &PeerId) -> CryptoHash {
        let mut guard = self.0.lock().unwrap();
        let route_id = CryptoHash::hash_borsh(guard.route_back.len());
        guard.route_back.insert(route_id, peer_id.clone());
        route_id
    }

    fn senders_for_route_back(&self, route_back: &CryptoHash) -> Arc<OneClientSenders> {
        let guard = self.0.lock().unwrap();
        let peer_id = guard.route_back.get(route_back).unwrap();
        guard.senders.get(peer_id).unwrap().clone()
    }

    fn accounts(&self) -> Vec<AccountId> {
        let guard = self.0.lock().unwrap();
        let account_ids = guard.account_to_peer_id.keys().cloned().collect_vec();
        account_ids
    }
}

impl Handler<SetChainInfo> for TestLoopPeerManagerActor {
    fn handle(&mut self, _msg: SetChainInfo) {}
}

impl Handler<StateSyncEvent> for TestLoopPeerManagerActor {
    fn handle(&mut self, _msg: StateSyncEvent) {}
}

impl Handler<Tier3Request> for TestLoopPeerManagerActor {
    fn handle(&mut self, _msg: Tier3Request) {}
}

impl Handler<PeerManagerMessageRequest> for TestLoopPeerManagerActor {
    fn handle(&mut self, msg: PeerManagerMessageRequest) -> PeerManagerMessageResponse {
        let PeerManagerMessageRequest::NetworkRequests(request) = msg else {
            panic!("Unexpected message: {:?}", msg);
        };

        // Iterate over the handlers in reverse order to allow for overriding the default handlers.
        let mut request = Some(request);
        for handler in self.handlers.iter().rev() {
            if let Some(new_request) = handler(request.take().unwrap()) {
                request = Some(new_request);
            } else {
                // Some handler was successfully able to handle the request.
                return PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse);
            }
        }
        // If no handler was able to handle the request, panic.
        panic!("Unhandled request: {:?}", request);
    }
}

fn network_message_to_client_handler(
    my_account_id: &AccountId,
    shared_state: TestLoopNetworkSharedState,
) -> NetworkRequestHandler {
    let my_account_id = my_account_id.clone();
    Box::new(move |request| match request {
        NetworkRequests::Block { block } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            for account_id in shared_state.accounts() {
                if account_id != my_account_id {
                    let future = shared_state
                        .senders_for_account(&account_id)
                        .client_sender
                        .send_async(BlockResponse {
                            block: block.clone(),
                            peer_id: my_peer_id.clone(),
                            was_requested: false,
                        });
                    drop(future);
                }
            }
            None
        }
        NetworkRequests::OptimisticBlock { optimistic_block } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            for account_id in shared_state.accounts() {
                if account_id != my_account_id {
                    let _ = shared_state.senders_for_account(&account_id).client_sender.send(
                        OptimisticBlockMessage {
                            optimistic_block: optimistic_block.clone(),
                            from_peer: my_peer_id.clone(),
                        },
                    );
                }
            }
            None
        }
        NetworkRequests::Approval { approval_message } => {
            assert_ne!(
                approval_message.target, my_account_id,
                "Sending message to self not supported."
            );
            let future = shared_state
                .senders_for_account(&approval_message.target)
                .client_sender
                .send_async(BlockApproval(approval_message.approval, PeerId::random()));
            drop(future);
            None
        }
        NetworkRequests::ForwardTx(account, transaction) => {
            assert_ne!(account, my_account_id, "Sending message to self not supported.");
            let future = shared_state.senders_for_account(&account).tx_processor_sender.send_async(
                ProcessTxRequest { transaction, is_forwarded: true, check_only: false },
            );
            drop(future);
            None
        }
        NetworkRequests::ChunkEndorsement(target, endorsement) => {
            let future = shared_state
                .senders_for_account(&target)
                .client_sender
                .send_async(ChunkEndorsementMessage(endorsement));
            drop(future);
            None
        }
        NetworkRequests::EpochSyncRequest { peer_id } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            assert_ne!(peer_id, my_peer_id, "Sending message to self not supported.");
            shared_state
                .senders_for_peer(&peer_id)
                .client_sender
                .send(EpochSyncRequestMessage { from_peer: my_peer_id });
            None
        }
        NetworkRequests::EpochSyncResponse { peer_id, proof } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            shared_state
                .senders_for_peer(&peer_id)
                .client_sender
                .send(EpochSyncResponseMessage { from_peer: my_peer_id, proof });
            None
        }
        NetworkRequests::StateRequestPart { .. } => None,
        NetworkRequests::Challenge(_) => None,
        _ => Some(request),
    })
}

fn network_message_to_view_client_handler(
    my_account_id: AccountId,
    shared_state: TestLoopNetworkSharedState,
    future_spawner: Arc<dyn FutureSpawner>,
) -> NetworkRequestHandler {
    Box::new(move |request| match request {
        NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
            let responder = shared_state.senders_for_account(&my_account_id).client_sender.clone();
            let future = shared_state
                .senders_for_peer(&peer_id)
                .view_client_sender
                .send_async(BlockHeadersRequest(hashes));
            future_spawner.spawn("wait for ViewClient to handle BlockHeadersRequest", async move {
                let response = future.await.unwrap().unwrap();
                let future = responder.send_async(BlockHeadersResponse(response, peer_id));
                drop(future);
            });
            None
        }
        NetworkRequests::BlockRequest { hash, peer_id } => {
            let responder = shared_state.senders_for_account(&my_account_id).client_sender.clone();
            let future = shared_state
                .senders_for_peer(&peer_id)
                .view_client_sender
                .send_async(BlockRequest(hash));
            future_spawner.spawn("wait for ViewClient to handle BlockRequest", async move {
                let response = *future.await.unwrap().unwrap();
                let future = responder.send_async(BlockResponse {
                    block: response,
                    peer_id,
                    was_requested: true,
                });
                drop(future);
            });
            None
        }
        _ => Some(request),
    })
}

fn network_message_to_partial_witness_handler(
    my_account_id: &AccountId,
    shared_state: TestLoopNetworkSharedState,
) -> NetworkRequestHandler {
    let my_account_id = my_account_id.clone();
    Box::new(move |request| match request {
        NetworkRequests::ChunkStateWitnessAck(target, witness_ack) => {
            assert_ne!(target, my_account_id, "Sending message to self not supported.");
            shared_state
                .senders_for_account(&target)
                .partial_witness_sender
                .send(ChunkStateWitnessAckMessage(witness_ack));
            None
        }

        NetworkRequests::PartialEncodedStateWitness(validator_witness_tuple) => {
            for (target, partial_witness) in validator_witness_tuple.into_iter() {
                shared_state
                    .senders_for_account(&target)
                    .partial_witness_sender
                    .send(PartialEncodedStateWitnessMessage(partial_witness));
            }
            None
        }
        NetworkRequests::PartialEncodedStateWitnessForward(chunk_validators, partial_witness) => {
            for target in chunk_validators {
                shared_state
                    .senders_for_account(&target)
                    .partial_witness_sender
                    .send(PartialEncodedStateWitnessForwardMessage(partial_witness.clone()));
            }
            None
        }
        NetworkRequests::ChunkContractAccesses(chunk_validators, accesses) => {
            for target in chunk_validators {
                shared_state
                    .senders_for_account(&target)
                    .partial_witness_sender
                    .send(ChunkContractAccessesMessage(accesses.clone()));
            }
            None
        }
        NetworkRequests::ContractCodeRequest(target, request) => {
            shared_state
                .senders_for_account(&target)
                .partial_witness_sender
                .send(ContractCodeRequestMessage(request));
            None
        }
        NetworkRequests::ContractCodeResponse(target, response) => {
            shared_state
                .senders_for_account(&target)
                .partial_witness_sender
                .send(ContractCodeResponseMessage(response));
            None
        }
        NetworkRequests::PartialEncodedContractDeploys(accounts, deploys) => {
            for account in accounts {
                shared_state
                    .senders_for_account(&account)
                    .partial_witness_sender
                    .send(PartialEncodedContractDeploysMessage(deploys.clone()));
            }
            None
        }
        _ => Some(request),
    })
}

fn network_message_to_state_snapshot_handler() -> NetworkRequestHandler {
    Box::new(move |request| match request {
        NetworkRequests::SnapshotHostInfo { .. } => None,
        _ => Some(request),
    })
}

fn network_message_to_shards_manager_handler(
    clock: Clock,
    my_account_id: &AccountId,
    shared_state: TestLoopNetworkSharedState,
) -> NetworkRequestHandler {
    let my_account_id = my_account_id.clone();
    Box::new(move |request| match request {
        NetworkRequests::PartialEncodedChunkRequest { target, request, .. } => {
            let my_peer_id = shared_state.account_to_peer_id(&my_account_id);
            let route_back = shared_state.generate_route_back(&my_peer_id);
            let target = target.account_id.unwrap();
            assert!(target != my_account_id, "Sending message to self not supported.");
            shared_state.senders_for_account(&target).shards_manager_sender.send(
                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                    partial_encoded_chunk_request: request,
                    route_back,
                },
            );
            None
        }
        NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
            // Use route_back information to send the response back to the correct client.
            shared_state.senders_for_route_back(&route_back).shards_manager_sender.send(
                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                    partial_encoded_chunk_response: response,
                    received_time: clock.now(),
                },
            );
            None
        }
        NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
            assert!(account_id != my_account_id, "Sending message to self not supported.");
            shared_state.senders_for_account(&account_id).shards_manager_sender.send(
                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                    partial_encoded_chunk.into(),
                ),
            );
            None
        }
        NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
            assert!(account_id != my_account_id, "Sending message to self not supported.");
            shared_state
                .senders_for_account(&account_id)
                .shards_manager_sender
                .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(forward));
            None
        }
        _ => Some(request),
    })
}
