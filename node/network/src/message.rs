use protobuf::{parse_from_bytes, Message as ProtoMessage, ProtobufResult};
use protobuf::{RepeatedField, SingularPtrField};
use serde_derive::{Deserialize, Serialize};
use std::convert::{TryFrom, TryInto};
use std::iter::FromIterator;

use mempool::payload_gossip::PayloadGossip;
use near_protos::chain as chain_proto;
use near_protos::network as network_proto;
use nightshade::nightshade_task::Gossip;
use primitives::beacon::SignedBeaconBlock;
use primitives::chain::{
    MissingPayloadRequest, MissingPayloadResponse, ReceiptBlock, SignedShardBlock, Snapshot,
};
use primitives::consensus::JointBlockBLS;
use primitives::hash::CryptoHash;
use primitives::network::ConnectedInfo;
use primitives::traits::Base58Encoded;
use primitives::transaction::SignedTransaction;
use primitives::types::{AuthorityId, BlockIndex};
use primitives::utils::proto_to_type;

pub type RequestId = u64;
pub type CoupledBlock = (SignedBeaconBlock, SignedShardBlock);

const PROTO_ERROR: &str = "Bad Proto";

/// Current latest version of the protocol
pub const PROTOCOL_VERSION: u32 = 1;

/// Message passed over the network from peer to peer.
/// Box's are used when message is significantly larger than other enum members.
#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
#[allow(clippy::large_enum_variant)]
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
    /// Response with list of blocks as well as the best chain index.
    BlockResponse(RequestId, Vec<CoupledBlock>, BlockIndex),

    /// Nightshade gossip.
    Gossip(Box<Gossip>),
    /// Announce of tx/receipts between authorities.
    PayloadGossip(Box<PayloadGossip>),
    /// Request specific tx/receipts.
    PayloadRequest(RequestId, MissingPayloadRequest),
    /// Response with payload for request.
    PayloadResponse(RequestId, MissingPayloadResponse),
    /// Request payload snapshot diff.
    PayloadSnapshotRequest(RequestId, CryptoHash),
    /// Response with snapshot for request.
    PayloadSnapshotResponse(RequestId, Snapshot),

    /// Partial BLS signatures of beacon and shard blocks.
    JointBlockBLS(JointBlockBLS),
}

impl TryFrom<network_proto::Message> for Message {
    type Error = String;

    fn try_from(proto: network_proto::Message) -> Result<Self, Self::Error> {
        match proto.message_type {
            Some(network_proto::Message_oneof_message_type::connected_info(info)) => {
                info.try_into().map(Message::Connected)
            }
            Some(network_proto::Message_oneof_message_type::transaction(tx)) => {
                tx.try_into().map(|tx| Message::Transaction(Box::new(tx)))
            }
            Some(network_proto::Message_oneof_message_type::receipt(receipt)) => {
                receipt.try_into().map(|receipt| Message::Receipt(Box::new(receipt)))
            }
            Some(network_proto::Message_oneof_message_type::block_announce(ann)) => {
                match (proto_to_type(ann.beacon_block), proto_to_type(ann.shard_block)) {
                    (Ok(beacon), Ok(shard)) => {
                        Ok(Message::BlockAnnounce(Box::new((beacon, shard))))
                    }
                    _ => Err(PROTO_ERROR.to_string()),
                }
            }
            Some(network_proto::Message_oneof_message_type::block_fetch_request(request)) => {
                Ok(Message::BlockFetchRequest(request.request_id, request.from, request.to))
            }
            Some(network_proto::Message_oneof_message_type::block_response(response)) => {
                let blocks: Result<Vec<_>, _> = response
                    .response
                    .into_iter()
                    .map(|coupled| {
                        match (
                            proto_to_type(coupled.beacon_block),
                            proto_to_type(coupled.shard_block),
                        ) {
                            (Ok(beacon), Ok(shard)) => Ok((beacon, shard)),
                            _ => Err(PROTO_ERROR.to_string()),
                        }
                    })
                    .collect();
                match blocks {
                    Ok(blocks) => {
                        Ok(Message::BlockResponse(response.request_id, blocks, response.best_index))
                    }
                    Err(e) => Err(e),
                }
            }
            Some(network_proto::Message_oneof_message_type::gossip(gossip)) => {
                gossip.try_into().map(|g| Message::Gossip(Box::new(g)))
            }
            Some(network_proto::Message_oneof_message_type::payload_gossip(payload_gossip)) => {
                payload_gossip.try_into().map(|g| Message::PayloadGossip(Box::new(g)))
            }
            Some(network_proto::Message_oneof_message_type::payload_request(request)) => {
                let payload_request = proto_to_type(request.payload)?;
                Ok(Message::PayloadRequest(request.request_id, payload_request))
            }
            Some(network_proto::Message_oneof_message_type::payload_snapshot_request(request)) => {
                Ok(Message::PayloadSnapshotRequest(
                    request.request_id,
                    request.snapshot_hash.try_into()?,
                ))
            }
            Some(network_proto::Message_oneof_message_type::payload_response(response)) => {
                match proto_to_type(response.payload) {
                    Ok(payload) => Ok(Message::PayloadResponse(response.request_id, payload)),
                    Err(e) => Err(e),
                }
            }
            Some(network_proto::Message_oneof_message_type::payload_snapshot_response(
                response,
            )) => {
                let snapshot = proto_to_type(response.snapshot)?;
                Ok(Message::PayloadSnapshotResponse(response.request_id, snapshot))
            }
            Some(network_proto::Message_oneof_message_type::joint_block_bls(joint)) => {
                match joint.field_type {
                    Some(network_proto::Message_JointBlockBLS_oneof_type::general(general)) => {
                        let beacon_sig = Base58Encoded::from_base58(&general.beacon_sig)
                            .map_err(|e| format!("cannot decode signature: {:?}", e))?;
                        let shard_sig = Base58Encoded::from_base58(&general.shard_sig)
                            .map_err(|e| format!("cannot deocde signature: {:?}", e))?;
                        Ok(Message::JointBlockBLS(JointBlockBLS::General {
                            sender_id: general.sender_id as AuthorityId,
                            receiver_id: general.receiver_id as AuthorityId,
                            beacon_hash: general.beacon_hash.try_into()?,
                            shard_hash: general.shard_hash.try_into()?,
                            beacon_sig,
                            shard_sig,
                        }))
                    }
                    Some(network_proto::Message_JointBlockBLS_oneof_type::request(request)) => {
                        Ok(Message::JointBlockBLS(JointBlockBLS::Request {
                            sender_id: request.sender_id as AuthorityId,
                            receiver_id: request.receiver_id as AuthorityId,
                            beacon_hash: request.beacon_hash.try_into()?,
                            shard_hash: request.shard_hash.try_into()?,
                        }))
                    }
                    None => unreachable!(),
                }
            }
            None => unreachable!(),
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
                    ..Default::default()
                };
                network_proto::Message_oneof_message_type::block_fetch_request(request)
            }
            Message::BlockResponse(request_id, blocks, best_index) => {
                let response = network_proto::Message_BlockResponse {
                    request_id,
                    response: RepeatedField::from_iter(blocks.into_iter().map(to_coupled_block)),
                    best_index,
                    ..Default::default()
                };
                network_proto::Message_oneof_message_type::block_response(response)
            }
            Message::Gossip(gossip) => {
                network_proto::Message_oneof_message_type::gossip((*gossip).into())
            }
            Message::PayloadGossip(payload_gossip) => {
                network_proto::Message_oneof_message_type::payload_gossip((*payload_gossip).into())
            }
            Message::PayloadRequest(request_id, request) => {
                let request = network_proto::Message_PayloadRequest {
                    request_id,
                    payload: SingularPtrField::some(request.into()),
                    ..Default::default()
                };
                network_proto::Message_oneof_message_type::payload_request(request)
            }
            Message::PayloadSnapshotRequest(request_id, snapshot_hash) => {
                let snapshot_request = network_proto::Message_PayloadSnapshotRequest {
                    request_id,
                    snapshot_hash: snapshot_hash.into(),
                    ..Default::default()
                };
                network_proto::Message_oneof_message_type::payload_snapshot_request(
                    snapshot_request,
                )
            }
            Message::PayloadResponse(request_id, payload) => {
                let response = network_proto::Message_PayloadResponse {
                    request_id,
                    payload: SingularPtrField::some(payload.into()),
                    ..Default::default()
                };
                network_proto::Message_oneof_message_type::payload_response(response)
            }
            Message::PayloadSnapshotResponse(request_id, snapshot) => {
                let response = network_proto::Message_PayloadSnapshotResponse {
                    request_id,
                    snapshot: SingularPtrField::some(snapshot.into()),
                    ..Default::default()
                };
                network_proto::Message_oneof_message_type::payload_snapshot_response(response)
            }
            Message::JointBlockBLS(joint_bls) => match joint_bls {
                JointBlockBLS::General {
                    sender_id,
                    receiver_id,
                    beacon_hash,
                    shard_hash,
                    beacon_sig,
                    shard_sig,
                } => {
                    let proto = network_proto::Message_JointBlockBLS_General {
                        sender_id: sender_id as u64,
                        receiver_id: receiver_id as u64,
                        beacon_hash: beacon_hash.into(),
                        shard_hash: shard_hash.into(),
                        beacon_sig: beacon_sig.to_base58(),
                        shard_sig: shard_sig.to_base58(),
                        ..Default::default()
                    };
                    let bls_proto = network_proto::Message_JointBlockBLS {
                        field_type: Some(network_proto::Message_JointBlockBLS_oneof_type::general(
                            proto,
                        )),
                        ..Default::default()
                    };
                    network_proto::Message_oneof_message_type::joint_block_bls(bls_proto)
                }
                JointBlockBLS::Request { sender_id, receiver_id, beacon_hash, shard_hash } => {
                    let proto = network_proto::Message_JointBlockBLS_Request {
                        sender_id: sender_id as u64,
                        receiver_id: receiver_id as u64,
                        beacon_hash: beacon_hash.into(),
                        shard_hash: shard_hash.into(),
                        unknown_fields: Default::default(),
                        cached_size: Default::default(),
                    };
                    let bls_proto = network_proto::Message_JointBlockBLS {
                        field_type: Some(network_proto::Message_JointBlockBLS_oneof_type::request(
                            proto,
                        )),
                        unknown_fields: Default::default(),
                        cached_size: Default::default(),
                    };
                    network_proto::Message_oneof_message_type::joint_block_bls(bls_proto)
                }
            },
        };
        network_proto::Message { message_type: Some(message_type), ..Default::default() }
    }
}

fn to_coupled_block(blocks: CoupledBlock) -> chain_proto::CoupledBlock {
    chain_proto::CoupledBlock {
        beacon_block: SingularPtrField::some(blocks.0.into()),
        shard_block: SingularPtrField::some(blocks.1.into()),
        ..Default::default()
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
