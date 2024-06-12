use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use near_async::messaging::{Actor, AsyncSender, CanSend, Handler, SendAsync, Sender};
use near_async::time::Clock;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use once_cell::sync::Lazy;

use crate::client::{
    BlockApproval, BlockResponse, ChunkEndorsementMessage, ProcessTxRequest, ProcessTxResponse,
};
use crate::shards_manager::ShardsManagerRequestFromNetwork;
use crate::state_witness::{
    ChunkStateWitnessAckMessage, PartialEncodedStateWitnessForwardMessage,
    PartialEncodedStateWitnessMessage, PartialWitnessSenderForNetwork,
};
use crate::types::{
    NetworkRequests, NetworkResponses, PeerManagerMessageRequest, PeerManagerMessageResponse,
    SetChainInfo,
};

/// Subset of ClientSenderForNetwork required for the TestLoop network.
/// We skip over the message handlers from view client.
#[derive(
    Clone, near_async::MultiSend, near_async::MultiSenderFrom, near_async::MultiSendMessage,
)]
#[multi_send_message_derive(Debug)]
#[multi_send_input_derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSenderForTestLoopNetwork {
    pub block: AsyncSender<BlockResponse, ()>,
    pub block_approval: AsyncSender<BlockApproval, ()>,
    pub transaction: AsyncSender<ProcessTxRequest, ProcessTxResponse>,
    pub chunk_endorsement: AsyncSender<ChunkEndorsementMessage, ()>,
}

type NetworkRequestHandler = Arc<dyn Fn(NetworkRequests) -> Option<NetworkRequests>>;

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
#[derive(Default)]
pub struct TestLoopPeerManagerActor {
    handlers: Vec<NetworkRequestHandler>,
}

impl Actor for TestLoopPeerManagerActor {}

impl TestLoopPeerManagerActor {
    /// Create a new TestLoopPeerManagerActor with default handlers for client, partial_witness, and shards_manager.
    /// Note that we should be able to access the senders for these actors from the data type.
    pub fn new<'a, T>(clock: Clock, account_id: &AccountId, datas: &'a Vec<T>) -> Self
    where
        AccountId: From<&'a T>,
        ClientSenderForTestLoopNetwork: From<&'a T>,
        PartialWitnessSenderForNetwork: From<&'a T>,
        Sender<ShardsManagerRequestFromNetwork>: From<&'a T>,
    {
        let handlers = vec![
            network_message_to_client_handler(&account_id, make_sender_map(datas)),
            network_message_to_partial_witness_handler(&account_id, make_sender_map(datas)),
            network_message_to_shards_manager_handler(clock, &account_id, make_sender_map(datas)),
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

// Helper function to create a map of senders from a list of data.
// Converts Vec<Data> to HashMap<AccountId, Sender>
fn make_sender_map<'a, T, U>(datas: &'a Vec<T>) -> HashMap<AccountId, U>
where
    AccountId: From<&'a T>,
    U: From<&'a T>,
{
    let mut senders = HashMap::new();
    for data in datas.iter() {
        senders.insert(data.into(), data.into());
    }
    senders
}

impl Handler<SetChainInfo> for TestLoopPeerManagerActor {
    fn handle(&mut self, _msg: SetChainInfo) {}
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
    client_senders: HashMap<AccountId, ClientSenderForTestLoopNetwork>,
) -> NetworkRequestHandler {
    let my_account_id = my_account_id.clone();
    Arc::new(move |request| match request {
        NetworkRequests::Block { block } => {
            for (account_id, sender) in client_senders.iter() {
                if account_id != &my_account_id {
                    let _ = sender.send_async(BlockResponse {
                        block: block.clone(),
                        peer_id: PeerId::random(),
                        was_requested: false,
                    });
                }
            }
            None
        }
        NetworkRequests::Approval { approval_message } => {
            assert_ne!(
                approval_message.target, my_account_id,
                "Sending message to self not supported."
            );
            let sender = client_senders.get(&approval_message.target).unwrap();
            let _ = sender.send_async(BlockApproval(approval_message.approval, PeerId::random()));
            None
        }
        NetworkRequests::ForwardTx(account, transaction) => {
            assert_ne!(account, my_account_id, "Sending message to self not supported.");
            let sender = client_senders.get(&account).unwrap();
            let _ = sender.send_async(ProcessTxRequest {
                transaction,
                is_forwarded: true,
                check_only: false,
            });
            None
        }
        NetworkRequests::ChunkEndorsement(target, endorsement) => {
            assert_ne!(target, my_account_id, "Sending message to self not supported.");
            let sender = client_senders.get(&target).unwrap();
            let _ = sender.send_async(ChunkEndorsementMessage(endorsement));
            None
        }
        _ => Some(request),
    })
}

fn network_message_to_partial_witness_handler(
    my_account_id: &AccountId,
    partial_witness_senders: HashMap<AccountId, PartialWitnessSenderForNetwork>,
) -> NetworkRequestHandler {
    let my_account_id = my_account_id.clone();
    Arc::new(move |request| match request {
        NetworkRequests::ChunkStateWitnessAck(target, witness_ack) => {
            assert_ne!(target, my_account_id, "Sending message to self not supported.");
            let sender = partial_witness_senders.get(&target).unwrap();
            sender.send(ChunkStateWitnessAckMessage(witness_ack));
            None
        }

        NetworkRequests::PartialEncodedStateWitness(validator_witness_tuple) => {
            for (target, partial_witness) in validator_witness_tuple.into_iter() {
                assert_ne!(target, my_account_id, "Sending message to self not supported.");
                let sender = partial_witness_senders.get(&target).unwrap();
                sender.send(PartialEncodedStateWitnessMessage(partial_witness));
            }
            None
        }
        NetworkRequests::PartialEncodedStateWitnessForward(chunk_validators, partial_witness) => {
            for target in chunk_validators {
                assert_ne!(target, my_account_id, "Sending message to self not supported.");
                let sender = partial_witness_senders.get(&target).unwrap();
                sender.send(PartialEncodedStateWitnessForwardMessage(partial_witness.clone()));
            }
            None
        }
        _ => Some(request),
    })
}

fn network_message_to_state_snapshot_handler() -> NetworkRequestHandler {
    Arc::new(move |request| match request {
        NetworkRequests::SnapshotHostInfo { .. } => None,
        _ => Some(request),
    })
}

/// While sending the PartialEncodedChunkRequest, we need to know the destination account id.
/// In the PartialEncodedChunkRequest, We specify the `route_back` as a unique identifier that is
/// used by the network layer to figure out who to send the response back to.
///
/// In network_message_to_shards_manager_handler fn, we use the static initialization for
/// ROUTE_LOOKUP. This is fine to use in the test framework as each generate route is unique and
/// independent of other routes.
#[derive(Default, Clone)]
struct PartialEncodedChunkRequestRouteLookup(Arc<Mutex<HashMap<CryptoHash, AccountId>>>);

impl PartialEncodedChunkRequestRouteLookup {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    // Generating route_id is under a lock and we use the size of hashmap to generate the route_id
    // The size of hashmap is strictly increasing which ensures us a unique route_id across multiple runs.
    fn add_route(&self, from_account_id: &AccountId) -> CryptoHash {
        let mut guard = self.0.lock().unwrap();
        let route_id = CryptoHash::hash_borsh(guard.len());
        guard.insert(route_id, from_account_id.clone());
        route_id
    }

    fn get_destination(&self, route_id: CryptoHash) -> AccountId {
        let guard = self.0.lock().unwrap();
        guard.get(&route_id).unwrap().clone()
    }
}

fn network_message_to_shards_manager_handler(
    clock: Clock,
    my_account_id: &AccountId,
    shards_manager_senders: HashMap<AccountId, Sender<ShardsManagerRequestFromNetwork>>,
) -> Arc<dyn Fn(NetworkRequests) -> Option<NetworkRequests>> {
    // Static initialization for ROUTE_LOOKUP. This is fine across tests as we generate a unique route_id
    // for each message under a lock.
    static ROUTE_LOOKUP: Lazy<PartialEncodedChunkRequestRouteLookup> =
        Lazy::new(PartialEncodedChunkRequestRouteLookup::new);
    let my_account_id = my_account_id.clone();
    Arc::new(move |request| match request {
        NetworkRequests::PartialEncodedChunkRequest { target, request, .. } => {
            // Save route information in ROUTE_LOOKUP
            let route_back = ROUTE_LOOKUP.add_route(&my_account_id);
            let target = target.account_id.unwrap();
            assert!(target != my_account_id, "Sending message to self not supported.");
            let sender = shards_manager_senders.get(&target).unwrap();
            sender.send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                partial_encoded_chunk_request: request,
                route_back,
            });
            None
        }
        NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
            // Use route_back information to send the response back to the correct client.
            let target = ROUTE_LOOKUP.get_destination(route_back);
            assert!(target != my_account_id, "Sending message to self not supported.");
            let sender = shards_manager_senders.get(&target).unwrap();
            sender.send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                partial_encoded_chunk_response: response,
                received_time: clock.now(),
            });
            None
        }
        NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
            assert!(account_id != my_account_id, "Sending message to self not supported.");
            let sender = shards_manager_senders.get(&account_id).unwrap();
            sender.send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                partial_encoded_chunk.into(),
            ));
            None
        }
        NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
            assert!(account_id != my_account_id, "Sending message to self not supported.");
            let sender = shards_manager_senders.get(&account_id).unwrap();
            sender
                .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(forward));
            None
        }
        _ => Some(request),
    })
}

/// A multi-instance test using the TestLoop framework can support routing
/// lookup for network messages, as long as the Data type contains AccountId.
/// This trait is just a helper for looking up the index.
pub trait SupportsRoutingLookup {
    fn index_for_account(&self, account: &AccountId) -> usize;
    fn num_accounts(&self) -> usize;
}

impl<InnerData: AsRef<AccountId>> SupportsRoutingLookup for Vec<InnerData> {
    fn index_for_account(&self, account: &AccountId) -> usize {
        self.iter()
            .position(|data| data.as_ref() == account)
            .unwrap_or_else(|| panic!("Account not found: {}", account))
    }

    fn num_accounts(&self) -> usize {
        self.len()
    }
}
