pub mod client_actor;
pub mod partial_witness_actor;
pub mod sync_actor;
pub mod sync_jobs_actor;

use crate::client_actor::{ClientActorInner, ClientSenderForPartialWitnessMessage};
use near_async::messaging::{CanSend, Handler, SendAsync};
use near_async::test_loop::delay_sender::DelaySender;
use near_async::test_loop::event_handler::{LoopEventHandler, TryIntoOrSelf};

use near_async::time::Duration;

use crate::Client;
use near_network::client::{
    BlockApproval, BlockResponse, ChunkEndorsementMessage, ClientSenderForNetwork,
    ClientSenderForNetworkMessage, ProcessTxRequest,
};
use near_network::state_witness::{
    ChunkStateWitnessAckMessage, PartialEncodedStateWitnessForwardMessage,
    PartialEncodedStateWitnessMessage, PartialWitnessSenderForNetwork,
    PartialWitnessSenderForNetworkMessage,
};
use near_network::test_loop::SupportsRoutingLookup;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, Balance, ShardId};
use near_primitives::views::{
    FinalExecutionOutcomeView, QueryRequest, QueryResponse, QueryResponseKind,
};

pub fn print_basic_client_info_before_each_event<Data, Event>(
    idx: Option<usize>,
) -> LoopEventHandler<Data, Event>
where
    Data: AsRef<ClientActorInner>,
{
    let idx_prefix = idx.map(|idx| format!("[Client #{}] ", idx)).unwrap_or_default();

    LoopEventHandler::new(move |msg, data: &mut Data| {
        let client = &data.as_ref().client;
        tracing::info!("{}sync_status: {:?}", idx_prefix, client.sync_status);
        let head = client.chain.head().unwrap();
        tracing::info!("{}Chain HEAD: {:?}", idx_prefix, head);

        if let Some(signer) = client.validator_signer.as_ref() {
            let account_id = signer.validator_id();

            let mut tracked_shards = Vec::new();
            let mut next_tracked_shards = Vec::new();
            let epoch_manager = client.epoch_manager.as_ref();
            for shard_id in &epoch_manager.shard_ids(&head.epoch_id).unwrap() {
                let tracks_shard = client
                    .epoch_manager
                    .cares_about_shard_from_prev_block(&head.prev_block_hash, account_id, *shard_id)
                    .unwrap();
                if tracks_shard {
                    tracked_shards.push(*shard_id);
                }
                let next_tracks_shard = client
                    .epoch_manager
                    .cares_about_shard_next_epoch_from_prev_block(
                        &head.prev_block_hash,
                        account_id,
                        *shard_id,
                    )
                    .unwrap();
                if next_tracks_shard {
                    next_tracked_shards.push(*shard_id);
                }
            }
            tracing::info!(
                "{}Validator assigned shards: this epoch = {:?}; next epoch = {:?}",
                idx_prefix,
                tracked_shards,
                next_tracked_shards
            );

            tracing::info!("{}Tx pool: {}", idx_prefix, client.sharded_tx_pool.debug_status());
        }
        Err(msg)
    })
}

/// Handles outgoing network messages, and turns them into incoming client messages.
pub fn route_network_messages_to_client<
    Data: SupportsRoutingLookup,
    Event: TryIntoOrSelf<PeerManagerMessageRequest>
        + From<PeerManagerMessageRequest>
        + From<ClientSenderForNetworkMessage>
        + From<PartialWitnessSenderForNetworkMessage>,
>(
    sender: DelaySender<(usize, Event)>,
    network_delay: Duration,
) -> LoopEventHandler<Data, (usize, Event)> {
    // let mut route_back_lookup: HashMap<CryptoHash, usize> = HashMap::new();
    // let mut next_hash: u64 = 0;
    LoopEventHandler::new(move |event: (usize, Event), data: &mut Data| {
        let (idx, event) = event;
        let message = event.try_into_or_self().map_err(|event| (idx, event.into()))?;
        let PeerManagerMessageRequest::NetworkRequests(request) = message else {
            return Err((idx, message.into()));
        };

        let client_senders = (0..data.num_accounts())
            .map(|idx| {
                sender
                    .with_additional_delay(network_delay)
                    .for_index(idx)
                    .into_wrapped_multi_sender::<ClientSenderForNetworkMessage, ClientSenderForNetwork>()
            })
            .collect::<Vec<_>>();

        let state_witness_senders = (0..data.num_accounts())
            .map(|idx| {
                sender
                    .with_additional_delay(network_delay)
                    .for_index(idx)
                    .into_wrapped_multi_sender::<PartialWitnessSenderForNetworkMessage, PartialWitnessSenderForNetwork>()
            })
            .collect::<Vec<_>>();

        match request {
            NetworkRequests::Block { block } => {
                for other_idx in 0..data.num_accounts() {
                    if other_idx != idx {
                        drop(client_senders[other_idx].send_async(BlockResponse {
                            block: block.clone(),
                            peer_id: PeerId::random(),
                            was_requested: false,
                        }));
                    }
                }
            }
            NetworkRequests::Approval { approval_message } => {
                let other_idx = data.index_for_account(&approval_message.target);
                if other_idx != idx {
                    drop(
                        client_senders[other_idx]
                            .send_async(BlockApproval(approval_message.approval, PeerId::random())),
                    );
                } else {
                    tracing::warn!("Dropping message to self");
                }
            }
            NetworkRequests::ForwardTx(account, transaction) => {
                let other_idx = data.index_for_account(&account);
                if other_idx != idx {
                    drop(client_senders[other_idx].send_async(ProcessTxRequest {
                        transaction,
                        is_forwarded: true,
                        check_only: false,
                    }))
                } else {
                    tracing::warn!("Dropping message to self");
                }
            }
            NetworkRequests::ChunkEndorsement(target, endorsement) => {
                let other_idx = data.index_for_account(&target);
                if other_idx != idx {
                    drop(
                        client_senders[other_idx].send_async(ChunkEndorsementMessage(endorsement)),
                    );
                } else {
                    tracing::warn!("Dropping message to self");
                }
            }
            NetworkRequests::ChunkStateWitnessAck(target, witness_ack) => {
                let other_idx = data.index_for_account(&target);
                if other_idx != idx {
                    state_witness_senders[other_idx].send(ChunkStateWitnessAckMessage(witness_ack));
                } else {
                    tracing::warn!("Dropping state-witness-ack message to self");
                }
            }
            NetworkRequests::PartialEncodedStateWitness(validator_witness_tuple) => {
                for (target, partial_witness) in validator_witness_tuple.into_iter() {
                    let other_idx = data.index_for_account(&target);
                    if other_idx != idx {
                        state_witness_senders[other_idx]
                            .send(PartialEncodedStateWitnessMessage(partial_witness));
                    } else {
                        tracing::warn!("Dropping state-witness message to self");
                    }
                }
            }
            NetworkRequests::PartialEncodedStateWitnessForward(
                chunk_validators,
                partial_witness,
            ) => {
                for target in chunk_validators {
                    let other_idx = data.index_for_account(&target);
                    if other_idx != idx {
                        state_witness_senders[other_idx].send(
                            PartialEncodedStateWitnessForwardMessage(partial_witness.clone()),
                        );
                    } else {
                        tracing::warn!("Dropping state-witness-forward message to self");
                    }
                }
            }
            NetworkRequests::SnapshotHostInfo { .. } => {
                // TODO: what to do about this?
            }
            // TODO: Support more network message types as we expand the test.
            _ => return Err((idx, PeerManagerMessageRequest::NetworkRequests(request).into())),
        }

        Ok(())
    })
}

// TODO: This would be a good starting point for turning this into a test util.
pub trait ClientQueries {
    fn client_index_tracking_account(&self, account: &AccountId) -> usize;
    fn runtime_query(&self, account: &AccountId, query: QueryRequest) -> QueryResponse;
    fn query_balance(&self, account: &AccountId) -> Balance;
    fn view_call(&self, account: &AccountId, method: &str, args: &[u8]) -> Vec<u8>;
    fn tx_outcome(&self, tx_hash: CryptoHash) -> FinalExecutionOutcomeView;
    fn tracked_shards_for_each_client(&self) -> Vec<Vec<ShardId>>;
}

impl<Data: AsRef<Client> + AsRef<AccountId>> ClientQueries for Vec<Data> {
    fn client_index_tracking_account(&self, account_id: &AccountId) -> usize {
        let client: &Client = self[0].as_ref();
        let head = client.chain.head().unwrap();
        let shard_id =
            client.epoch_manager.account_id_to_shard_id(&account_id, &head.epoch_id).unwrap();

        for i in 0..self.len() {
            let client: &Client = self[i].as_ref();
            let tracks_shard = client
                .epoch_manager
                .cares_about_shard_from_prev_block(
                    &head.prev_block_hash,
                    &self[i].as_ref(),
                    shard_id,
                )
                .unwrap();
            if tracks_shard {
                return i;
            }
        }
        panic!("No client tracks shard {}", shard_id);
    }

    fn runtime_query(&self, account_id: &AccountId, query: QueryRequest) -> QueryResponse {
        let client_index = self.client_index_tracking_account(account_id);
        let client: &Client = self[client_index].as_ref();
        let head = client.chain.head().unwrap();
        let last_block = client.chain.get_block(&head.last_block_hash).unwrap();
        let shard_id =
            client.epoch_manager.account_id_to_shard_id(&account_id, &head.epoch_id).unwrap();
        let shard_uid = client.epoch_manager.shard_id_to_uid(shard_id, &head.epoch_id).unwrap();
        let last_chunk_header = &last_block.chunks()[shard_id as usize];

        client
            .runtime_adapter
            .query(
                shard_uid,
                &last_chunk_header.prev_state_root(),
                last_block.header().height(),
                last_block.header().raw_timestamp(),
                last_block.header().prev_hash(),
                last_block.header().hash(),
                last_block.header().epoch_id(),
                &query,
            )
            .unwrap()
    }

    fn query_balance(&self, account_id: &AccountId) -> Balance {
        let response = self.runtime_query(
            account_id,
            QueryRequest::ViewAccount { account_id: account_id.clone() },
        );
        if let QueryResponseKind::ViewAccount(account_view) = response.kind {
            account_view.amount
        } else {
            panic!("Wrong return value")
        }
    }

    fn view_call(&self, account_id: &AccountId, method: &str, args: &[u8]) -> Vec<u8> {
        let response = self.runtime_query(
            account_id,
            QueryRequest::CallFunction {
                account_id: account_id.clone(),
                method_name: method.to_string(),
                args: args.to_vec().into(),
            },
        );
        if let QueryResponseKind::CallResult(call_result) = response.kind {
            call_result.result
        } else {
            panic!("Wrong return value")
        }
    }

    fn tx_outcome(&self, tx_hash: CryptoHash) -> FinalExecutionOutcomeView {
        // TODO: this does not work yet with single-shard tracking.
        let client: &Client = self[0].as_ref();
        client.chain.get_final_transaction_result(&tx_hash).unwrap()
    }

    fn tracked_shards_for_each_client(&self) -> Vec<Vec<ShardId>> {
        let client: &Client = self[0].as_ref();
        let head = client.chain.head().unwrap();
        let all_shard_ids = client.epoch_manager.shard_ids(&head.epoch_id).unwrap();

        let mut ret = Vec::new();
        for i in 0..self.len() {
            let client: &Client = self[i].as_ref();
            let mut tracked_shards = Vec::new();
            for shard_id in &all_shard_ids {
                let tracks_shard = client
                    .epoch_manager
                    .cares_about_shard_from_prev_block(
                        &head.prev_block_hash,
                        &self[i].as_ref(),
                        *shard_id,
                    )
                    .unwrap();
                if tracks_shard {
                    tracked_shards.push(*shard_id);
                }
            }
            ret.push(tracked_shards);
        }
        ret
    }
}

pub fn forward_messages_from_partial_witness_actor_to_client(
) -> LoopEventHandler<ClientActorInner, ClientSenderForPartialWitnessMessage> {
    LoopEventHandler::new_simple(|msg, client_actor: &mut ClientActorInner| match msg {
        ClientSenderForPartialWitnessMessage::_chunk_state_witness(msg) => client_actor.handle(msg),
    })
}
