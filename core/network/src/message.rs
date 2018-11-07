use primitives::{types, traits::{Encode, Decode}};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum MessageBody {
    //TODO: add different types of messages here
    Transaction(types::SignedTransaction),
    Status(Status),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Message {
    pub src: u32,
    pub dst: u32,
    pub channel: String,
    pub body: MessageBody,
}

/// status sent on connection
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    // protocol version
    pub version: u32,
    // TODO: block number, block hash, etc
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

    /// for now, we are not using the other fields.
    pub fn new_default(body: MessageBody) -> Message {
        Message {
            src: 0,
            dst: 0,
            channel: String::new(),
            body
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