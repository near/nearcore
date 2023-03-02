use std::collections::HashMap;

use near_async::test_loop::event_handler::{
    interval, LoopEventHandler, LoopHandlerContext, TryIntoOrSelf,
};
use near_network::{
    shards_manager::ShardsManagerRequestFromNetwork,
    test_loop::SupportsRoutingLookup,
    types::{NetworkRequests, PeerManagerMessageRequest},
};
use near_primitives::hash::CryptoHash;
use near_primitives::time;

use crate::{adapter::ShardsManagerRequestFromClient, ShardsManager};

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
pub struct ShardsManagerResendRequests;

/// Periodically call resend_chunk_requests.
pub fn periodically_resend_shards_manager_requests(
    every: time::Duration,
) -> LoopEventHandler<ShardsManager, ShardsManagerResendRequests> {
    interval(every, ShardsManagerResendRequests, |data: &mut ShardsManager| {
        data.resend_chunk_requests()
    })
}
