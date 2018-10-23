/// network protocol

use primitives::types;
use network::message;

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

struct Protocol {
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

    fn on_message(&self, message: message::Message) {
        match message.body {
            message::MessageBody::TransactionMessage(tx) => self.on_transaction_message(tx)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    #[should_panic]
    fn sanity_check() {
        let tx = types::SignedTransaction::new(0, 0, types::TransactionBody::new(0, 0, 0, 0));
        let message = message::Message::new(
            0, 0, "shard0", message::MessageBody::TransactionMessage(tx)
        );
        let config = ProtocolConfig::new("1.0");
        let protocol = Protocol::new(config);
        protocol.on_message(message)
    }

}