use std::io;

use beacon::types::{SignedBeaconBlock, SignedBeaconBlockHeader};
use primitives::serialize::{
    decode_proto, encode_proto, Decode, DecodeResult, Encode, EncodeResult
};
use primitives::types::{BlockId, Gossip};
use transaction::{ChainPayload, ReceiptTransaction, SignedTransaction};

use near_protos::message as message_proto;

pub type RequestId = u64;

#[derive(PartialEq, Debug)]
pub enum Message {
    // Box is used here because SignedTransaction
    // is significantly larger than other enum members
    Transaction(Box<SignedTransaction>),
    Receipt(Box<ReceiptTransaction>),
    Status(message_proto::Status),
    BlockRequest(BlockRequest),
    BlockResponse(BlockResponse<SignedBeaconBlock>),
    BlockAnnounce(BlockAnnounce<SignedBeaconBlock, SignedBeaconBlockHeader>),
    Gossip(Box<Gossip<ChainPayload>>),
}

impl Encode for Message {
    fn encode(&self) -> EncodeResult {
        let mut m = message_proto::Message::new();
        match &self {
            Message::Transaction(t) => m.set_transaction(t.encode()?),
            Message::Receipt(t) => m.set_receipt(t.encode()?),
            Message::Status(status) => m.set_status(status.clone()),
            Message::BlockRequest(req) => m.set_block_request(req.encode()?),
            Message::BlockResponse(res) => m.set_block_response(res.encode()?),
            Message::BlockAnnounce(ann) => m.set_block_announce(ann.encode()?),
            Message::Gossip(gossip) => m.set_gossip(gossip.encode()?),
        };
        Ok(encode_proto(&m)?)
    }
}

impl Decode for Message {
    fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let m: message_proto::Message = decode_proto(bytes)?;
        match m.message_type {
            Some(message_proto::Message_oneof_message_type::transaction(t)) => Ok(Message::Transaction(Box::new(Decode::decode(&t)?))),
            Some(message_proto::Message_oneof_message_type::receipt(t)) => Ok(Message::Receipt(Box::new(Decode::decode(&t)?))),
            Some(message_proto::Message_oneof_message_type::status(status)) => Ok(Message::Status(status)),
            Some(message_proto::Message_oneof_message_type::block_request(x)) => Ok(Message::BlockRequest(Decode::decode(&x)?)),
            Some(message_proto::Message_oneof_message_type::block_response(x)) => Ok(Message::BlockResponse(Decode::decode(&x)?)),
            Some(message_proto::Message_oneof_message_type::block_announce(x)) => Ok(Message::BlockAnnounce(Decode::decode(&x)?)),
            Some(message_proto::Message_oneof_message_type::gossip(x)) => Ok(Message::Gossip(Box::new(Decode::decode(&x)?))),
            _ => Err(io::Error::new(io::ErrorKind::Other, "Found unknown type or empty Message at deserialization"))
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct BlockRequest {
    /// request id
    pub id: RequestId,
    /// starting from this id
    pub from: BlockId,
    /// ending at this id,
    pub to: Option<BlockId>,
    /// max number of blocks requested
    pub max: Option<u64>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockResponse<Block> {
    // request id that the response is responding to
    pub id: RequestId,
    // block data
    pub blocks: Vec<Block>,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum BlockAnnounce<B, H> {
    // Announce either header or the entire block
    Header(H),
    Block(B),
}
