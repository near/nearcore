extern crate serde;
extern crate serde_json;

use primitives::types;


#[derive(Serialize, Deserialize)]
pub enum MessageBody {
    //TODO: add different types of messages here
    TransactionMessage(types::SignedTransaction) // placeholder here
}

#[derive(Serialize, Deserialize)]
pub struct Message {
    src: u32,
    dst: u32,
    channel: &'static str,
    pub body: MessageBody,
}

impl Message {
    pub fn new(src: u32, dst: u32, channel: &'static str, body: MessageBody) -> Message {
        Message {
            src,
            dst,
            channel,
            body
        }
    }
}