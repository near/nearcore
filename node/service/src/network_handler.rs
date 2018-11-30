use client::Client;
use network::protocol::ProtocolHandler;
use primitives::traits::GenericResult;
use primitives::types::SignedTransaction;
use std::sync::Arc;

pub struct NetworkHandler {
    pub client: Arc<Client>,
}

impl ProtocolHandler for NetworkHandler {
    fn handle_transaction(&self, t: SignedTransaction) -> GenericResult {
        self.client.handle_signed_transaction(t)
    }
}
