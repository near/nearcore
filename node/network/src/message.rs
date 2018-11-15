use primitives::traits::{Decode, Encode};
use primitives::hash::CryptoHash;
use protocol::Transaction;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum MessageBody<T> {
    //TODO: add different types of messages here
    Transaction(T),
    Status(Status),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Message<T> {
    pub body: MessageBody<T>,
}

/// status sent on connection
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    // protocol version
    pub version: u32,
    // best block number
    pub best_number: u64,
    // best block hash
    pub best_hash: CryptoHash,
    // genesis hash
    pub genesis_hash: CryptoHash,
}

impl Default for Status {
    fn default() -> Self {
        Status {
            version: 1,
            best_number: 0,
            best_hash: CryptoHash { 0: [0; 32] },
            genesis_hash: CryptoHash { 0: [0; 32] }
        }
    }
}

impl<T> Message<T> {
    pub fn new(body: MessageBody<T>) -> Message<T> {
        Message { body }
    }
}

impl<T: Transaction> Encode for Message<T> {
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

impl<T: Transaction> Decode for Message<T> {
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
