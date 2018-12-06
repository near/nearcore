use client::Client;
use protocol::ProtocolHandler;
use primitives::traits::GenericResult;
use primitives::types::{SignedTransaction, ReceiptTransaction};
use std::sync::Arc;

pub struct NetworkHandler {
    pub client: Arc<Client>,
}

impl ProtocolHandler for NetworkHandler {
    fn handle_transaction(&self, t: SignedTransaction) -> GenericResult {
        self.client.handle_signed_transaction(t)
    }

    fn handle_receipt(&self, receipt: ReceiptTransaction) -> GenericResult {
        self.client.handle_receipt_transaction(receipt)
    }
}
