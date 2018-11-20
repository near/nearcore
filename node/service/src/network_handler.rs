use client::Client;
use network::protocol::ProtocolHandler;
use network::protocol::Transaction;
use primitives::traits::GenericResult;
use std::sync::Arc;

pub struct NetworkHandler {
    pub client: Arc<Client>,
}

impl ProtocolHandler for NetworkHandler {
    fn handle_transaction<T: Transaction>(&self, t: &T) -> GenericResult {
        self.client.handle_signed_transaction(t)
    }
}
