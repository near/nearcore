use client::Client;
use protocol::ProtocolHandler;
use primitives::traits::GenericResult;
use primitives::types::{SignedTransaction, ReceiptTransaction};
use std::sync::Arc;
use parking_lot::RwLock;

use futures::sync::mpsc;
use futures::{Future, Sink};

pub struct NetworkHandler {
    pub client: Arc<RwLock<Client>>,
}

impl ProtocolHandler for NetworkHandler {
    fn handle_transaction(&self, t: SignedTransaction) -> GenericResult {
        self.client.write().handle_signed_transaction(t)
    }

    fn handle_receipt(&self, receipt: ReceiptTransaction) -> GenericResult {
        self.client.write().handle_receipt_transaction(receipt)
    }
}

/// Reports transactions received from a network into the given channel.
pub struct ChannelNetworkHandler {
    transactions_sender: mpsc::Sender<SignedTransaction>,
    receipts_sender: mpsc::Sender<ReceiptTransaction>,
}

impl ChannelNetworkHandler {
    pub fn new(
        transactions_sender: mpsc::Sender<SignedTransaction>,
        receipts_sender: mpsc::Sender<ReceiptTransaction>
    ) -> Self {
        Self { transactions_sender, receipts_sender }
    }
}

impl ProtocolHandler for ChannelNetworkHandler {
    fn handle_transaction(&self, transaction: SignedTransaction) -> Result<(), &'static str> {
        let copied_tx = self.transactions_sender.clone();
        tokio::spawn(
            copied_tx
                .send(transaction)
                .map(|_| ())
                .map_err(|e| error!("Failure to send the transactions {:?}", e)),
        );
        Ok(())
    }

    fn handle_receipt(&self, receipt: ReceiptTransaction) -> GenericResult {
        let copied_tx = self.receipts_sender.clone();
        tokio::spawn(
            copied_tx
                .send(receipt)
                .map(|_| ())
                .map_err(|e| error!("Failure to send the receipts {:?}", e)),
        );
        Ok(())
    }
}
