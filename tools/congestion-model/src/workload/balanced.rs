use crate::{GGas, ReceiptDefinition, ShardId, TransactionBuilder, TGAS};

use super::{utils, Producer};

/// Simple transaction producer that sends N receipts from each shard to all other shards every round.
pub struct BalancedProducer {
    pub receipt_size: u64,
    pub attached_gas: GGas,
    pub execution_gas: GGas,
    pub conversion_gas: GGas,
    pub n_receipts_per_shard_pair: usize,
}

impl Producer for BalancedProducer {
    fn init(&mut self, _shards: &[ShardId]) {}

    fn produce_transactions(
        &mut self,
        _round: crate::Round,
        shards: &[ShardId],
        tx_factory: &mut dyn FnMut(ShardId) -> TransactionBuilder,
    ) -> Vec<TransactionBuilder> {
        let mut out = vec![];
        for _ in 0..self.n_receipts_per_shard_pair {
            for sender in shards {
                for receiver in shards {
                    let mut tx = tx_factory(*sender);
                    self.produce_one_tx(*receiver, &mut tx);
                    out.push(tx);
                }
            }
        }
        out
    }
}

impl BalancedProducer {
    fn produce_one_tx(&self, receiver: ShardId, tx: &mut TransactionBuilder) {
        let receipt = ReceiptDefinition {
            receiver,
            size: self.receipt_size,
            attached_gas: self.attached_gas,
            execution_gas: self.attached_gas,
        };
        let first = tx.add_first_receipt(receipt, self.conversion_gas);
        tx.new_outgoing_receipt(first, utils::refund_receipt(tx.sender_shard()));
    }
}

impl Default for BalancedProducer {
    fn default() -> Self {
        // for this simple scenario, as a default, use 1kb sized receipts with 300 Tgas attached that
        // end up actually executing 5 TGas + 100 TGas.
        Self {
            receipt_size: 1024,
            attached_gas: 300 * TGAS,
            execution_gas: 100 * TGAS,
            conversion_gas: 5 * TGAS,
            n_receipts_per_shard_pair: 2,
        }
    }
}
