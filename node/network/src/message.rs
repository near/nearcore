use serde_derive::{Deserialize, Serialize};
use protobuf::{RepeatedField, SingularPtrField};
use std::iter::FromIterator;
use std::convert::{TryInto, TryFrom};
use protobuf::{Message as ProtoMessage, ProtobufResult, parse_from_bytes};

use nightshade::nightshade_task::Gossip;
use mempool::payload_gossip::PayloadGossip;
use primitives::beacon::SignedBeaconBlock;
use primitives::chain::{ChainPayload, ReceiptBlock, SignedShardBlock};
use primitives::hash::CryptoHash;
use primitives::transaction::SignedTransaction;
use primitives::network::ConnectedInfo;
use primitives::utils::proto_to_type;
use near_protos::network as network_proto;
use near_protos::chain as chain_proto;

pub type RequestId = u64;
pub type CoupledBlock = (SignedBeaconBlock, SignedShardBlock);

const PROTO_ERROR: &str = "Bad Proto";

/// Current latest version of the protocol
pub const PROTOCOL_VERSION: u32 = 1;

/// Message passed over the network from peer to peer.
/// Box's are used when message is significantly larger than other enum members.
#[derive(PartialEq, Debug, Serialize, Deserialize)]
pub enum Message {
    /// On peer connected, information about their chain.
    Connected(ConnectedInfo),
    /// Incoming transaction.
    Transaction(Box<SignedTransaction>),
    /// Incoming receipt block.
    Receipt(Box<ReceiptBlock>),

    /// Announce of new block.
    BlockAnnounce(Box<CoupledBlock>),
    /// Fetch range of blocks by index.
    BlockFetchRequest(RequestId, u64, u64),
    /// Response with list of blocks.
    BlockResponse(RequestId, Vec<CoupledBlock>),

    /// Nightshade gossip.
    Gossip(Box<Gossip>),
    /// Announce of tx/receipts between authorities.
    PayloadGossip(Box<PayloadGossip>),
    /// Request specific tx/receipts.
    PayloadRequest(RequestId, Vec<CryptoHash>, Vec<CryptoHash>),
    /// Request payload snapshot diff.
    PayloadSnapshotRequest(RequestId, CryptoHash),
    /// Response with payload for request.
    PayloadResponse(RequestId, ChainPayload),
}

impl TryFrom<network_proto::Message> for Message {
    type Error = String;

    fn try_from(proto: network_proto::Message) -> Result<Self, Self::Error> {
        match proto.message_type {
            Some(network_proto::Message_oneof_message_type::connected_info(info)) => {
                info.try_into().map(Message::Connected)
            }
            Some(network_proto::Message_oneof_message_type::transaction(tx)) => {
                Ok(Message::Transaction(Box::new(tx.into())))
            }
            Some(network_proto::Message_oneof_message_type::receipt(receipt)) => {
                receipt.try_into().map(|receipt| Message::Receipt(Box::new(receipt)))
            }
            Some(network_proto::Message_oneof_message_type::block_announce(ann)) => {
                match (proto_to_type(ann.beacon_block), proto_to_type(ann.shard_block)) {
                    (Ok(beacon), Ok(shard)) => {
                        Ok(Message::BlockAnnounce(Box::new((beacon, shard))))
                    }
                    _ => Err(PROTO_ERROR.to_string())
                }
            }
            Some(network_proto::Message_oneof_message_type::block_fetch_request(request)) => {
                Ok(Message::BlockFetchRequest(request.request_id, request.from, request.to))
            }
            Some(network_proto::Message_oneof_message_type::block_response(response)) => {
                let blocks: Result<Vec<_>, _> = response.response
                    .into_iter()
                    .map(|coupled| {
                        match (proto_to_type(coupled.beacon_block), proto_to_type(coupled.shard_block)) {
                            (Ok(beacon), Ok(shard)) => {
                                Ok((beacon, shard))
                            }
                            _ => Err(PROTO_ERROR.to_string())
                        }
                    })
                    .collect();
                match blocks {
                    Ok(blocks) => Ok(Message::BlockResponse(response.request_id, blocks)),
                    Err(e) => Err(e)
                }
            }
            Some(network_proto::Message_oneof_message_type::gossip(gossip)) => {
                gossip.try_into().map(|g| Message::Gossip(Box::new(g)))
            }
            Some(network_proto::Message_oneof_message_type::payload_gossip(payload_gossip)) => {
                payload_gossip.try_into().map(|g| Message::PayloadGossip(Box::new(g)))
            }
            Some(network_proto::Message_oneof_message_type::payload_request(request)) => {
                let transaction_hashes = request.transaction_hashes
                    .into_iter()
                    .map(std::convert::Into::into)
                    .collect();
                let receipt_hashes = request.receipt_hashes
                    .into_iter()
                    .map(std::convert::Into::into)
                    .collect();
                Ok(Message::PayloadRequest(request.request_id, transaction_hashes, receipt_hashes))
            }
            Some(network_proto::Message_oneof_message_type::payload_snapshot_request(request)) => {
                Ok(Message::PayloadSnapshotRequest(request.request_id, request.snapshot_hash.into()))
            }
            Some(network_proto::Message_oneof_message_type::payload_response(response)) => {
                match proto_to_type(response.payload) {
                    Ok(payload) => Ok(Message::PayloadResponse(response.request_id, payload)),
                    Err(e) => Err(e)
                }
            }
            None => unreachable!()
        }
    }
}

impl From<Message> for network_proto::Message {
    fn from(message: Message) -> Self {
        let message_type = match message {
            Message::Connected(connected_info) => {
                network_proto::Message_oneof_message_type::connected_info(connected_info.into())
            }
            Message::Transaction(tx) => {
                network_proto::Message_oneof_message_type::transaction((*tx).into())
            }
            Message::Receipt(receipt) => {
                network_proto::Message_oneof_message_type::receipt((*receipt).into())
            }
            Message::BlockAnnounce(ann) => {
                let blocks = to_coupled_block(*ann);
                network_proto::Message_oneof_message_type::block_announce(blocks)
            }
            Message::BlockFetchRequest(request_id, from, to) => {
                let request = network_proto::Message_BlockFetchRequest {
                    request_id,
                    from,
                    to,
                    unknown_fields: Default::default(),
                    cached_size: Default::default(),
                };
                network_proto::Message_oneof_message_type::block_fetch_request(request)
            }
            Message::BlockResponse(request_id, blocks) => {
                let response = network_proto::Message_BlockResponse {
                    request_id,
                    response: RepeatedField::from_iter(
                        blocks.into_iter().map(to_coupled_block)
                    ),
                    unknown_fields: Default::default(),
                    cached_size: Default::default(),
                };
                network_proto::Message_oneof_message_type::block_response(response)
            }
            Message::Gossip(gossip) => {
                network_proto::Message_oneof_message_type::gossip((*gossip).into())
            }
            Message::PayloadGossip(payload_gossip) => {
                network_proto::Message_oneof_message_type::payload_gossip((*payload_gossip).into())
            }
            Message::PayloadRequest(request_id, transaction_hashes, receipt_hashes) => {
                let request = network_proto::Message_PayloadRequest {
                    request_id,
                    transaction_hashes: RepeatedField::from_iter(
                        transaction_hashes.into_iter().map(std::convert::Into::into)
                    ),
                    receipt_hashes: RepeatedField::from_iter(
                        receipt_hashes.into_iter().map(std::convert::Into::into)
                    ),
                    unknown_fields: Default::default(),
                    cached_size: Default::default(),
                };
                network_proto::Message_oneof_message_type::payload_request(request)
            }
            Message::PayloadSnapshotRequest(request_id, snapshot_hash) => {
                let snapshot_request = network_proto::Message_PayloadSnapshotRequest {
                    request_id,
                    snapshot_hash: snapshot_hash.into(),
                    unknown_fields: Default::default(),
                    cached_size: Default::default(),
                };
                network_proto::Message_oneof_message_type::payload_snapshot_request(snapshot_request)
            }
            Message::PayloadResponse(request_id, payload) => {
                let response = network_proto::Message_PayloadResponse {
                    request_id,
                    payload: SingularPtrField::some(payload.into()),
                    unknown_fields: Default::default(),
                    cached_size: Default::default(),
                };
                network_proto::Message_oneof_message_type::payload_response(response)
            }
        };
        network_proto::Message {
            message_type: Some(message_type),
            unknown_fields: Default::default(),
            cached_size: Default::default(),
        }
    }
}

fn to_coupled_block(blocks: CoupledBlock) -> chain_proto::CoupledBlock {
    chain_proto::CoupledBlock {
        beacon_block: SingularPtrField::some(blocks.0.into()),
        shard_block: SingularPtrField::some(blocks.1.into()),
        unknown_fields: Default::default(),
        cached_size: Default::default(),
    }
}

pub fn encode_message(message: Message) -> ProtobufResult<Vec<u8>> {
    let proto: network_proto::Message = message.into();
    proto.write_to_bytes()
}

pub fn decode_message(data: &[u8]) -> Result<Message, String> {
    parse_from_bytes::<network_proto::Message>(data)
        .map_err(|e| format!("Protobuf error: {}", e))
        .and_then(TryInto::try_into)
}

