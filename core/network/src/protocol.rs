/// network protocol

use substrate_network_libp2p::{Service};
use primitives::{types, traits::{Encode, Decode}};
use message::{self, Message};
use parking_lot::Mutex;
use std::sync::Arc;

struct ProtocolConfig {
    // config information goes here
    version: &'static str,
}

impl ProtocolConfig {
    fn new(version: &'static str) -> ProtocolConfig {
        ProtocolConfig {
            version
        }
    }
}

pub struct Protocol {
    // TODO: add more fields when we need them
    config: ProtocolConfig,
}

impl Protocol {
    fn new(config: ProtocolConfig) -> Protocol {
        Protocol {
            config
        }
    }
    
    fn on_transaction_message(&self, tx: types::SignedTransaction) {
        panic!("TODO: transaction message");
    }

    pub fn on_message(&self, network: &Arc<Mutex<Service>>, mut data: &[u8]) {
        let message: Message = match Decode::decode(data) {
            Some(m) => m,
            _ => {
                println!("cannot decode message: {:?}", data);
                return;
            }
        };
        match message.body {
            message::MessageBody::TransactionMessage(tx) => self.on_transaction_message(tx)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    impl Protocol {
        fn _on_message(&self, mut data: &[u8]) -> Message {
            match Decode::decode(data) {
                Some(m) => m,
                _ => panic!("cannot decode message: {:?}", data)
            }
        }
    }

    #[test]
    fn test_serialization() {
        let tx = types::SignedTransaction::new(0, 0, types::TransactionBody::new(0, 0, 0, 0));
        let message = message::Message::new(
            0, 0, "shard0", message::MessageBody::TransactionMessage(tx)
        );
        let config = ProtocolConfig::new("1.0");
        let protocol = Protocol::new(config);
        let decoded = protocol._on_message(&Encode::encode(&message).unwrap());
        assert_eq!(message, decoded);
    }

}