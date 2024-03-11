use crate::{GGas, ReceiptDefinition, ShardId, TransactionBuilder, TGAS};

use super::{utils, Producer};

/// Simple transaction producer that sends receipts from all shards to shard 0.
pub struct AllForOneProducer {
    // config options
    pub receipt_size: u64,
    pub attached_gas: GGas,
    pub execution_gas: GGas,
    pub conversion_gas: GGas,
    pub messages_per_round: usize,

    // state
    pub round_robin_shards: Box<dyn Iterator<Item = ShardId>>,
}

impl Producer for AllForOneProducer {
    fn init(&mut self, shards: &[ShardId]) {
        // note: the `to_vec` is required to satisfy lifetimes
        #[allow(clippy::unnecessary_to_owned)]
        let iter = shards.to_vec().into_iter().cycle();
        self.round_robin_shards = Box::new(iter);
    }

    fn produce_transactions(
        &mut self,
        _round: crate::Round,
        shards: &[ShardId],
        tx_factory: &mut dyn FnMut(ShardId) -> TransactionBuilder,
    ) -> Vec<TransactionBuilder> {
        (0..self.messages_per_round)
            .map(|_| {
                let shard = self.round_robin_shards.next().unwrap();
                let mut tx = tx_factory(shard);
                self.produce_one_tx(shards, &mut tx);
                tx
            })
            .collect()
    }
}

impl AllForOneProducer {
    fn produce_one_tx(&self, shards: &[ShardId], tx: &mut TransactionBuilder) {
        let first = tx.add_first_receipt(self.receipt_to_shard_0(shards), self.conversion_gas);
        tx.new_outgoing_receipt(first, utils::refund_receipt(tx.sender_shard()));
    }

    fn receipt_to_shard_0(&self, shards: &[ShardId]) -> ReceiptDefinition {
        ReceiptDefinition {
            receiver: shards[0],
            size: self.receipt_size,
            attached_gas: self.attached_gas,
            execution_gas: self.attached_gas,
        }
    }
}

impl Default for AllForOneProducer {
    fn default() -> Self {
        // for this simple scenario, as a default, use 1kb sized receipts with 300 Tgas attached that
        // end up actually executing 5 TGas + 100 TGas.
        Self {
            receipt_size: 1024,
            attached_gas: 300 * TGAS,
            execution_gas: 100 * TGAS,
            conversion_gas: 5 * TGAS,
            messages_per_round: 20,
            // empty iterator overwritten in init
            round_robin_shards: Box::new(std::iter::empty()),
        }
    }
}
