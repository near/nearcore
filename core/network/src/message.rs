use primitives::{types, traits::{Encode, Decode}};
use serde_json;
use bincode;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum MessageBody {
    //TODO: add different types of messages here
    TransactionMessage(types::SignedTransaction) // placeholder here
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Message {
    src: u32,
    dst: u32,
    channel: String,
    pub body: MessageBody,
}

impl Message {
    pub fn new(src: u32, dst: u32, channel: &str, body: MessageBody) -> Message {
        Message {
            src,
            dst,
            body,
            channel: channel.to_string()
        }
    }
}

impl Encode for Message {
    fn encode(&self) -> Option<Vec<u8>> {
        match bincode::serialize(&self) {
            Ok(data) => Some(data),
            Err(e) => {
                error!("error occurred while encoding: {:?}", e);
                None
            }
        }
    }
}

impl Decode for Message {
    fn decode(data: &[u8]) -> Option<Self> {
        // need to figure out how to deserialize without copying
        match bincode::deserialize(data) {
            Ok(s) => Some(s),
            Err(e) => {
                error!("error occurred while decoding: {:?}", e);
                None
            }
        }
    }
}